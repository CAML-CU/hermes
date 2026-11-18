//! Hermes NATS Core transport. All messages are ephemeral (no JetStream or replay).
//! Call these APIs inside a Tokio runtime. NATS publish success means the client
//! accepted the publish, not that a subscriber processed it.

use std::time::Duration;

use futures_util::StreamExt;
use prost::Message;
use tokio::sync::{mpsc, oneshot};

pub mod subject;

const MAX_SNAPSHOT_BYTES: usize = 1024 * 1024;

/// Prost messages compiled from `../../proto/trading/v1/*.proto` at build time.
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/trading.v1.rs"));
}


#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid NATS subject token: {0:?} (use only ASCII letters, digits, _ or -)")]
    InvalidSubjectToken(String),
    #[error("emission queue capacity must be positive")]
    InvalidCapacity,
    #[error("a Tokio runtime is required")]
    NoRuntime,
    #[error("emission queue is full")]
    QueueFull,
    #[error("emission queue is closed")]
    QueueClosed,


    #[error("invalid message: {0}")]
    InvalidMessage(&'static str),
    #[error("request timed out")]
    Timeout,
    #[error("protobuf decode failed: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("NATS transport error: {0}")]
    Transport(String),
}

struct QueuedEmission {
    subject: String,
    payload: Vec<u8>,
    result: oneshot::Sender<Result<(), Error>>,
}

/// Cloneable NATS client with a bounded, non-blocking bot-emission queue.
#[derive(Clone)]
pub struct HermesCore {
    client: async_nats::Client,
    emissions: mpsc::Sender<QueuedEmission>,
}

/// Await this to learn whether an enqueued emission was accepted by the NATS client.
/// Dropping it does not cancel the publish. This is not a subscriber acknowledgment.
pub struct EmissionReceipt(oneshot::Receiver<Result<(), Error>>);

impl EmissionReceipt {
    pub async fn wait(self) -> Result<(), Error> {
        self.0.await.map_err(|_| Error::QueueClosed)?
    }
}

impl HermesCore {
    pub async fn connect(url: &str, emission_capacity: usize) -> Result<Self, Error> {
        let client = async_nats::connect(url)
            .await
            .map_err(|err| Error::Transport(err.to_string()))?;
        Self::from_client(client, emission_capacity)
    }

    /// Must be called within a Tokio runtime; spawns one worker per instance.
    pub fn from_client(client: async_nats::Client, emission_capacity: usize) -> Result<Self, Error> {
        if emission_capacity == 0 {
            return Err(Error::InvalidCapacity);
        }
        let (tx, mut rx) = mpsc::channel::<QueuedEmission>(emission_capacity);
        let publisher = client.clone();
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| Error::NoRuntime)?;
        runtime.spawn(async move {
            while let Some(item) = rx.recv().await {
                let result = publisher
                    .publish(item.subject, item.payload.into())
                    .await
                    .map_err(|err| Error::Transport(err.to_string()));
                let _ = item.result.send(result);
            }
        });
        Ok(Self {
            client,
            emissions: tx,
        })
    }

    /// Enqueue without waiting; returns `QueueFull` rather than blocking the producer.
    /// Ordering is FIFO across clones of this instance. A receipt reports publish errors.
    pub fn try_publish_emission(&self, emission: proto::BotEmission) -> Result<EmissionReceipt, Error> {
        let subject = subject::bot_emissions(&emission.bot_id)?;
        if emission.emission_type.is_empty() {
            return Err(Error::InvalidMessage("emission_type is required"));
        }
        let (tx, rx) = oneshot::channel();
        self.emissions
            .try_send(QueuedEmission {
                subject,
                payload: emission.encode_to_vec(),
                result: tx,
            })
            .map_err(|err| match err {
                mpsc::error::TrySendError::Full(_) => Error::QueueFull,
                mpsc::error::TrySendError::Closed(_) => Error::QueueClosed,
            })?;
        Ok(EmissionReceipt(rx))
    }

    pub async fn subscribe_bot_emissions(
        &self,
        bot_id: &str,
    ) -> Result<BotEmissionSubscription, Error> {
        let subject = subject::bot_emissions(bot_id)?;
        let subscriber = self
            .client
            .subscribe(subject)
            .await
            .map_err(|err| Error::Transport(err.to_string()))?;
        Ok(BotEmissionSubscription {
            subscriber,
            bot_id: bot_id.to_owned(),
        })
    }

    /// Publish a bot heartbeat on the bots-only subject.
    pub async fn publish_bot_heartbeat(&self, heartbeat: proto::BotHeartbeat) -> Result<(), Error> {
        validate_bot_heartbeat(&heartbeat)?;
        self.client
            .publish(subject::BOT_HEARTBEATS, heartbeat.encode_to_vec().into())
            .await
            .map_err(|err| Error::Transport(err.to_string()))
    }

    /// Publish a wrangler heartbeat on the wranglers-only subject.
    pub async fn publish_wrangler_heartbeat(
        &self,
        heartbeat: proto::WranglerHeartbeat,
    ) -> Result<(), Error> {
        self.client
            .publish(subject::WRANGLER_HEARTBEATS, heartbeat.encode_to_vec().into())
            .await
            .map_err(|err| Error::Transport(err.to_string()))
    }

    pub async fn subscribe_bot_heartbeats(&self) -> Result<BotHeartbeatSubscription, Error> {
        let subscriber = self
            .client
            .subscribe(subject::BOT_HEARTBEATS)
            .await
            .map_err(|err| Error::Transport(err.to_string()))?;
        Ok(BotHeartbeatSubscription { subscriber })
    }

    pub async fn subscribe_wrangler_heartbeats(&self) -> Result<WranglerHeartbeatSubscription, Error> {
        let subscriber = self
            .client
            .subscribe(subject::WRANGLER_HEARTBEATS)
            .await
            .map_err(|err| Error::Transport(err.to_string()))?;
        Ok(WranglerHeartbeatSubscription { subscriber })
    }

    /// Assigns a UUID command_id when empty, and verifies both identifiers in the reply.
    /// A timeout cannot cancel server-side execution; retrying may execute a command twice.
    pub async fn request_bot_command(
        &self,
        mut command: proto::BotCommand,
        timeout: Duration,
    ) -> Result<proto::BotCommandResponse, Error> {
        let subject = subject::bot_commands(&command.bot_id)?;
        if command.command_id.is_empty() {
            command.command_id = uuid::Uuid::new_v4().to_string();
        }
        let response = tokio::time::timeout(
            timeout,
            self.client.request(subject, command.encode_to_vec().into()),
        )
        .await
        .map_err(|_| Error::Timeout)?
        .map_err(|err| Error::Transport(err.to_string()))?;
        let response = proto::BotCommandResponse::decode(response.payload)?;
        validate_response(&command, &response)?;
        Ok(response)
    }

    /// Requests the bot's current active orders. A timeout may also mean the bot
    /// could not send a reply (for example, because its snapshot exceeded 1 MiB).
    pub async fn request_active_orders(
        &self,
        bot_id: &str,
        timeout: Duration,
    ) -> Result<proto::BotActiveOrdersSnapshot, Error> {
        let subject = subject::bot_orders_snapshot(bot_id)?;
        let request = proto::BotActiveOrdersRequest { bot_id: bot_id.to_owned() };
        let response = tokio::time::timeout(
            timeout,
            self.client.request(subject, request.encode_to_vec().into()),
        )
        .await
        .map_err(|_| Error::Timeout)?
        .map_err(|err| Error::Transport(err.to_string()))?;
        validate_snapshot_size(response.payload.len())?;
        let snapshot = proto::BotActiveOrdersSnapshot::decode(response.payload)?;
        validate_snapshot(&request, &snapshot)?;
        Ok(snapshot)
    }

    pub async fn subscribe_active_orders_requests(
        &self,
        bot_id: &str,
    ) -> Result<ActiveOrdersRequestSubscription, Error> {
        let subscriber = self.client
            .subscribe(subject::bot_orders_snapshot(bot_id)?)
            .await
            .map_err(|err| Error::Transport(err.to_string()))?;
        Ok(ActiveOrdersRequestSubscription {
            subscriber,
            bot_id: bot_id.to_owned(),
            client: self.client.clone(),
        })
    }

    pub async fn publish_orders_changed(
        &self,
        notification: proto::BotOrdersChanged,
    ) -> Result<(), Error> {
        let subject = subject::bot_orders_changed(&notification.bot_id)?;
        validate_orders_changed(&notification)?;
        self.client
            .publish(subject, notification.encode_to_vec().into())
            .await
            .map_err(|err| Error::Transport(err.to_string()))
    }

    pub async fn subscribe_orders_changed(
        &self,
        bot_id: &str,
    ) -> Result<OrdersChangedSubscription, Error> {
        let subscriber = self.client
            .subscribe(subject::bot_orders_changed(bot_id)?)
            .await
            .map_err(|err| Error::Transport(err.to_string()))?;
        Ok(OrdersChangedSubscription { subscriber, bot_id: bot_id.to_owned() })
    }

    pub async fn subscribe_bot_commands(
        &self,
        bot_id: &str,
    ) -> Result<BotCommandSubscription, Error> {
        let subject = subject::bot_commands(bot_id)?;
        let subscriber = self
            .client
            .subscribe(subject)
            .await
            .map_err(|err| Error::Transport(err.to_string()))?;
        Ok(BotCommandSubscription {
            subscriber,
            bot_id: bot_id.to_owned(),
            client: self.client.clone(),
        })
    }


}

pub struct ActiveOrdersRequestSubscription {
    subscriber: async_nats::Subscriber,
    bot_id: String,
    client: async_nats::Client,
}

impl ActiveOrdersRequestSubscription {
    /// Malformed requests return an error; later messages can still be read.
    pub async fn next(&mut self) -> Result<Option<IncomingActiveOrdersRequest>, Error> {
        let Some(message) = self.subscriber.next().await else {
            return Ok(None);
        };
        let request = proto::BotActiveOrdersRequest::decode(message.payload)?;
        if request.bot_id != self.bot_id {
            return Err(Error::InvalidMessage("active orders request bot_id mismatch"));
        }
        let reply = message.reply.ok_or(Error::InvalidMessage("active orders request has no reply subject"))?;
        Ok(Some(IncomingActiveOrdersRequest { request, reply, client: self.client.clone() }))
    }
}

pub struct IncomingActiveOrdersRequest {
    pub request: proto::BotActiveOrdersRequest,
    reply: async_nats::Subject,
    client: async_nats::Client,
}

impl IncomingActiveOrdersRequest {
    /// Only a matching, versioned snapshot of at most 1 MiB can be sent.
    pub async fn respond(self, snapshot: proto::BotActiveOrdersSnapshot) -> Result<(), Error> {
        validate_snapshot(&self.request, &snapshot)?;
        validate_snapshot_size(snapshot.encoded_len())?;
        self.client
            .publish(self.reply, snapshot.encode_to_vec().into())
            .await
            .map_err(|err| Error::Transport(err.to_string()))
    }
}

pub struct OrdersChangedSubscription {
    subscriber: async_nats::Subscriber,
    bot_id: String,
}

impl OrdersChangedSubscription {
    /// Malformed notifications return an error; later messages can still be read.
    pub async fn next(&mut self) -> Result<Option<proto::BotOrdersChanged>, Error> {
        let Some(message) = self.subscriber.next().await else {
            return Ok(None);
        };
        let notification = proto::BotOrdersChanged::decode(message.payload)?;
        validate_received_orders_changed(&notification, &self.bot_id)?;
        Ok(Some(notification))
    }
}

fn validate_snapshot(
    request: &proto::BotActiveOrdersRequest,
    snapshot: &proto::BotActiveOrdersSnapshot,
) -> Result<(), Error> {
    if snapshot.bot_id != request.bot_id {
        return Err(Error::InvalidMessage("snapshot bot_id mismatch"));
    }
    if snapshot.generation_id.is_empty() || snapshot.version == 0 {
        return Err(Error::InvalidMessage("snapshot requires generation_id and positive version"));
    }
    Ok(())
}

fn validate_snapshot_size(size: usize) -> Result<(), Error> {
    if size > MAX_SNAPSHOT_BYTES {
        return Err(Error::InvalidMessage("snapshot exceeds 1 MiB"));
    }
    Ok(())
}

fn validate_received_orders_changed(notification: &proto::BotOrdersChanged, bot_id: &str) -> Result<(), Error> {
    validate_orders_changed(notification)?;
    if notification.bot_id != bot_id {
        return Err(Error::InvalidMessage("orders changed bot_id mismatch"));
    }
    Ok(())
}

fn validate_orders_changed(notification: &proto::BotOrdersChanged) -> Result<(), Error> {
    if notification.generation_id.is_empty() || notification.version == 0 {
        return Err(Error::InvalidMessage("orders changed requires generation_id and positive version"));
    }
    Ok(())
}

pub struct BotHeartbeatSubscription {
    subscriber: async_nats::Subscriber,
}

impl BotHeartbeatSubscription {
    /// An invalid heartbeat is reported as an error; later messages can still be read.
    pub async fn next(&mut self) -> Result<Option<proto::BotHeartbeat>, Error> {
        let Some(message) = self.subscriber.next().await else {
            return Ok(None);
        };
        let heartbeat = proto::BotHeartbeat::decode(message.payload)?;
        validate_bot_heartbeat(&heartbeat)?;
        Ok(Some(heartbeat))
    }
}

pub struct WranglerHeartbeatSubscription {
    subscriber: async_nats::Subscriber,
}

impl WranglerHeartbeatSubscription {
    /// An invalid heartbeat is reported as an error; later messages can still be read.
    pub async fn next(&mut self) -> Result<Option<proto::WranglerHeartbeat>, Error> {
        let Some(message) = self.subscriber.next().await else {
            return Ok(None);
        };
        let heartbeat = proto::WranglerHeartbeat::decode(message.payload)?;
        Ok(Some(heartbeat))
    }
}

fn validate_bot_heartbeat(heartbeat: &proto::BotHeartbeat) -> Result<(), Error> {
    if heartbeat.bot_id.is_empty() {
        return Err(Error::InvalidMessage("bot heartbeat requires a bot_id"));
    }
    Ok(())
}


pub struct BotEmissionSubscription {
    subscriber: async_nats::Subscriber,
    bot_id: String,
}

impl BotEmissionSubscription {
    /// `Ok(None)` means the subscription ended. Malformed messages return an error.
    pub async fn next(&mut self) -> Result<Option<proto::BotEmission>, Error> {
        let Some(message) = self.subscriber.next().await else {
            return Ok(None);
        };
        let emission = proto::BotEmission::decode(message.payload)?;
        if emission.bot_id != self.bot_id {
            return Err(Error::InvalidMessage("emission bot_id mismatch"));
        }
        Ok(Some(emission))
    }
}

pub struct BotCommandSubscription {
    subscriber: async_nats::Subscriber,
    bot_id: String,
    client: async_nats::Client,
}

impl BotCommandSubscription {
    /// Each command carries a one-shot NATS reply subject. Handle commands sequentially
    /// or move the returned `IncomingBotCommand` into a spawned task for concurrency.
    pub async fn next(&mut self) -> Result<Option<IncomingBotCommand>, Error> {
        let Some(message) = self.subscriber.next().await else {
            return Ok(None);
        };
        let command = proto::BotCommand::decode(message.payload)?;
        if command.bot_id != self.bot_id || command.command_id.is_empty() {
            return Err(Error::InvalidMessage("command bot_id mismatch or missing command_id"));
        }
        let reply = message
            .reply
            .ok_or(Error::InvalidMessage("command has no reply subject"))?;
        Ok(Some(IncomingBotCommand {
            command,
            reply,
            client: self.client.clone(),
        }))
    }
}

pub struct IncomingBotCommand {
    pub command: proto::BotCommand,
    reply: async_nats::Subject,
    client: async_nats::Client,
}

impl IncomingBotCommand {
    /// Reply using the same bot_id and command_id received in `command`.
    pub async fn respond(self, response: proto::BotCommandResponse) -> Result<(), Error> {
        validate_response(&self.command, &response)?;
        self.client
            .publish(self.reply, response.encode_to_vec().into())
            .await
            .map_err(|err| Error::Transport(err.to_string()))
    }
}

fn validate_response(command: &proto::BotCommand, response: &proto::BotCommandResponse) -> Result<(), Error> {
    if response.bot_id != command.bot_id || response.command_id != command.command_id {
        return Err(Error::InvalidMessage("response bot_id or command_id mismatch"));
    }
    Ok(())
}


#[cfg(test)]
mod tests;
