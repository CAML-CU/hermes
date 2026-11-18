use std::{
    sync::{Arc, Mutex as StdMutex, RwLock},
    time::Duration,
};

use hermes_core::{
    proto, ActiveOrdersRequestSubscription, BotEmissionSubscription, BotHeartbeatSubscription,
    EmissionReceipt, HermesCore, IncomingActiveOrdersRequest, OrdersChangedSubscription,
    WranglerHeartbeatSubscription,
};
use prost_types::{value::Kind, ListValue, Struct, Value};
use pyo3::{
    create_exception,
    exceptions::{PyRuntimeError, PyValueError},
    prelude::*,
};
use serde::Deserialize;
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;

create_exception!(_native, HermesError, PyRuntimeError);
create_exception!(_native, QueueFullError, HermesError);

fn core_error(error: hermes_core::Error) -> PyErr {
    match error {
        hermes_core::Error::QueueFull => QueueFullError::new_err(error.to_string()),
        hermes_core::Error::InvalidCapacity
        | hermes_core::Error::InvalidSubjectToken(_)
        | hermes_core::Error::InvalidMessage(_) => PyValueError::new_err(error.to_string()),
        _ => HermesError::new_err(error.to_string()),
    }
}

fn json_value(value: serde_json::Value) -> Value {
    let kind = match value {
        serde_json::Value::Null => Kind::NullValue(0),
        serde_json::Value::Bool(value) => Kind::BoolValue(value),
        serde_json::Value::Number(value) => Kind::NumberValue(value.as_f64().expect("JSON number")),
        serde_json::Value::String(value) => Kind::StringValue(value),
        serde_json::Value::Array(values) => Kind::ListValue(ListValue {
            values: values.into_iter().map(json_value).collect(),
        }),
        serde_json::Value::Object(fields) => Kind::StructValue(Struct {
            fields: fields
                .into_iter()
                .map(|(key, value)| (key, json_value(value)))
                .collect(),
        }),
    };
    Value { kind: Some(kind) }
}

fn parse_data(json: &str) -> PyResult<Struct> {
    let value: serde_json::Value = serde_json::from_str(json)
        .map_err(|err| PyValueError::new_err(format!("invalid data_json: {err}")))?;
    match json_value(value).kind {
        Some(Kind::StructValue(data)) => Ok(data),
        _ => Err(PyValueError::new_err("data_json must be a JSON object")),
    }
}

fn to_json(value: Value) -> serde_json::Value {
    match value.kind {
        None | Some(Kind::NullValue(_)) => serde_json::Value::Null,
        Some(Kind::BoolValue(value)) => serde_json::Value::Bool(value),
        Some(Kind::NumberValue(value)) => serde_json::Number::from_f64(value)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Some(Kind::StringValue(value)) => serde_json::Value::String(value),
        Some(Kind::ListValue(list)) => {
            serde_json::Value::Array(list.values.into_iter().map(to_json).collect())
        }
        Some(Kind::StructValue(data)) => serde_json::Value::Object(
            data.fields
                .into_iter()
                .map(|(key, value)| (key, to_json(value)))
                .collect(),
        ),
    }
}

fn struct_json(data: Option<Struct>) -> String {
    let value = data
        .map(|data| {
            to_json(Value {
                kind: Some(Kind::StructValue(data)),
            })
        })
        .unwrap_or_else(|| serde_json::json!({}));
    value.to_string()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OrderInput {
    venue: String,
    symbol: String,
    order_id: String,
    client_order_id: String,
    side: String,
    status: String,
    limit_price: Option<String>,
    total_quantity: String,
    filled_quantity: String,
}

fn parse_orders(json: &str) -> PyResult<Vec<proto::ActiveOrder>> {
    let orders: Vec<OrderInput> = serde_json::from_str(json)
        .map_err(|err| PyValueError::new_err(format!("invalid orders_json: {err}")))?;
    Ok(orders
        .into_iter()
        .map(|order| proto::ActiveOrder {
            venue: order.venue,
            symbol: order.symbol,
            order_id: order.order_id,
            client_order_id: order.client_order_id,
            side: order.side,
            status: order.status,
            limit_price: order.limit_price,
            total_quantity: order.total_quantity,
            filled_quantity: order.filled_quantity,
        })
        .collect())
}

fn orders_json(orders: Vec<proto::ActiveOrder>) -> String {
    serde_json::Value::Array(
        orders
            .into_iter()
            .map(|order| {
                serde_json::json!({
                    "venue": order.venue,
                    "symbol": order.symbol,
                    "order_id": order.order_id,
                    "client_order_id": order.client_order_id,
                    "side": order.side,
                    "status": order.status,
                    "limit_price": order.limit_price,
                    "total_quantity": order.total_quantity,
                    "filled_quantity": order.filled_quantity,
                })
            })
            .collect(),
    )
    .to_string()
}

#[pyclass(module = "hermes._native")]
struct Hermes {
    core: HermesCore,
}

#[pymethods]
impl Hermes {
    #[staticmethod]
    #[pyo3(signature = (url, emission_capacity=1024))]
    fn connect<'py>(
        py: Python<'py>,
        url: String,
        emission_capacity: usize,
    ) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let core = HermesCore::connect(&url, emission_capacity)
                .await
                .map_err(core_error)?;
            Python::with_gil(|py| Py::new(py, Self { core }).map(|value| value.into_py(py)))
        })
    }

    #[pyo3(signature = (bot_id, emission_type, data_json="{}"))]
    fn try_emit(
        &self,
        py: Python<'_>,
        bot_id: String,
        emission_type: String,
        data_json: &str,
    ) -> PyResult<Receipt> {
        let data = parse_data(data_json)?;
        let core = self.core.clone();
        let receipt = py
            .allow_threads(move || {
                core.try_publish_emission(proto::BotEmission {
                    bot_id,
                    emission_type,
                    data: Some(data),
                })
            })
            .map_err(core_error)?;
        Ok(Receipt {
            receipt: Mutex::new(Some(receipt)),
        })
    }

    #[pyo3(signature = (bot_id, started_at, heartbeat_interval_seconds=5.0, config_json="{}"))]
    fn start_bot<'py>(
        &self,
        py: Python<'py>,
        bot_id: String,
        started_at: u64,
        heartbeat_interval_seconds: f64,
        config_json: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        if !heartbeat_interval_seconds.is_finite() || heartbeat_interval_seconds <= 0.0 {
            return Err(PyValueError::new_err(
                "heartbeat_interval_seconds must be finite and positive",
            ));
        }
        let heartbeat_interval = Duration::try_from_secs_f64(heartbeat_interval_seconds)
            .map_err(|_| PyValueError::new_err("heartbeat_interval_seconds is too large"))?;
        let config = parse_data(config_json)?;
        let core = self.core.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let subscription = core
                .subscribe_bot_commands(&bot_id)
                .await
                .map_err(core_error)?;
            let state = Arc::new(BotSessionState {
                core,
                bot_id,
                started_at,
                heartbeat_interval,
                mutable: RwLock::new(BotSessionMutable {
                    active: true,
                    status: proto::Status::WarmingUp as i32,
                    paused_at: None,
                    config,
                }),
                callback: StdMutex::new(None),
                changed: Notify::new(),
                shutdown: Notify::new(),
                tasks: Mutex::new(Vec::new()),
            });
            let _ = state.core.publish_bot_heartbeat(state.heartbeat()).await;
            let heartbeat_state = state.clone();
            let heartbeat_task = tokio::spawn(async move {
                heartbeat_loop(heartbeat_state).await;
            });
            let command_state = state.clone();
            let command_task = tokio::spawn(async move {
                command_loop(command_state, subscription).await;
            });
            state
                .tasks
                .lock()
                .await
                .extend([heartbeat_task, command_task]);
            Python::with_gil(|py| Py::new(py, BotSession { state }).map(|value| value.into_py(py)))
        })
    }

    #[pyo3(signature = (bot_id, status, started_at, paused_at=None, config_json="{}"))]
    fn publish_bot_heartbeat<'py>(
        &self,
        py: Python<'py>,
        bot_id: String,
        status: i32,
        started_at: u64,
        paused_at: Option<u64>,
        config_json: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let config_schema = Some(parse_data(config_json)?);
        let core = self.core.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core.publish_bot_heartbeat(proto::BotHeartbeat {
                bot_id,
                status,
                started_at,
                paused_at,
                config_schema,
            })
            .await
            .map_err(core_error)
        })
    }

    #[pyo3(signature = (status, started_at, paused_at=None))]
    fn publish_wrangler_heartbeat<'py>(
        &self,
        py: Python<'py>,
        status: i32,
        started_at: u64,
        paused_at: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core.publish_wrangler_heartbeat(proto::WranglerHeartbeat {
                status,
                started_at,
                paused_at,
            })
            .await
            .map_err(core_error)
        })
    }

    fn subscribe_bot_heartbeats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let sub = core.subscribe_bot_heartbeats().await.map_err(core_error)?;
            Python::with_gil(|py| {
                Py::new(
                    py,
                    BotHeartbeats {
                        sub: Arc::new(Mutex::new(sub)),
                    },
                )
                .map(|value| value.into_py(py))
            })
        })
    }

    fn subscribe_wrangler_heartbeats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let sub = core
                .subscribe_wrangler_heartbeats()
                .await
                .map_err(core_error)?;
            Python::with_gil(|py| {
                Py::new(
                    py,
                    WranglerHeartbeats {
                        sub: Arc::new(Mutex::new(sub)),
                    },
                )
                .map(|value| value.into_py(py))
            })
        })
    }

    fn subscribe_bot_emissions<'py>(
        &self,
        py: Python<'py>,
        bot_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let sub = core
                .subscribe_bot_emissions(&bot_id)
                .await
                .map_err(core_error)?;
            Python::with_gil(|py| {
                Py::new(
                    py,
                    Emissions {
                        sub: Arc::new(Mutex::new(sub)),
                    },
                )
                .map(|value| value.into_py(py))
            })
        })
    }

    #[pyo3(signature = (bot_id, command_type, data_json="{}", command_id=String::new(), timeout_seconds=5.0))]
    fn request_bot_command<'py>(
        &self,
        py: Python<'py>,
        bot_id: String,
        command_type: i32,
        data_json: &str,
        command_id: String,
        timeout_seconds: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let data = parse_data(data_json)?;
        if !timeout_seconds.is_finite() || timeout_seconds <= 0.0 {
            return Err(PyValueError::new_err(
                "timeout_seconds must be finite and positive",
            ));
        }
        let timeout = Duration::try_from_secs_f64(timeout_seconds)
            .map_err(|_| PyValueError::new_err("timeout_seconds is too large"))?;
        let core = self.core.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = core
                .request_bot_command(
                    proto::BotCommand {
                        bot_id,
                        r#type: command_type,
                        data: Some(data),
                        command_id,
                    },
                    timeout,
                )
                .await
                .map_err(core_error)?;
            Python::with_gil(|py| {
                Py::new(
                    py,
                    CommandResponse {
                        bot_id: response.bot_id,
                        command_id: response.command_id,
                        success: response.success,
                        message_json: struct_json(response.message),
                        error: response.error,
                    },
                )
                .map(|value| value.into_py(py))
            })
        })
    }

    #[pyo3(signature = (bot_id, timeout_seconds=5.0))]
    fn request_active_orders<'py>(
        &self,
        py: Python<'py>,
        bot_id: String,
        timeout_seconds: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        if !timeout_seconds.is_finite() || timeout_seconds <= 0.0 {
            return Err(PyValueError::new_err(
                "timeout_seconds must be finite and positive",
            ));
        }
        let timeout = Duration::try_from_secs_f64(timeout_seconds)
            .map_err(|_| PyValueError::new_err("timeout_seconds is too large"))?;
        let core = self.core.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let snapshot = core
                .request_active_orders(&bot_id, timeout)
                .await
                .map_err(core_error)?;
            Python::with_gil(|py| {
                Py::new(py, ActiveOrdersSnapshot::from_proto(snapshot))
                    .map(|value| value.into_py(py))
            })
        })
    }

    fn subscribe_active_orders_requests<'py>(
        &self,
        py: Python<'py>,
        bot_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let sub = core
                .subscribe_active_orders_requests(&bot_id)
                .await
                .map_err(core_error)?;
            Python::with_gil(|py| {
                Py::new(
                    py,
                    ActiveOrdersRequests {
                        sub: Arc::new(Mutex::new(sub)),
                    },
                )
                .map(|value| value.into_py(py))
            })
        })
    }

    fn publish_orders_changed<'py>(
        &self,
        py: Python<'py>,
        bot_id: String,
        generation_id: String,
        version: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core.publish_orders_changed(proto::BotOrdersChanged {
                bot_id,
                generation_id,
                version,
            })
            .await
            .map_err(core_error)
        })
    }

    fn subscribe_orders_changed<'py>(
        &self,
        py: Python<'py>,
        bot_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let sub = core
                .subscribe_orders_changed(&bot_id)
                .await
                .map_err(core_error)?;
            Python::with_gil(|py| {
                Py::new(
                    py,
                    OrdersChanged {
                        sub: Arc::new(Mutex::new(sub)),
                    },
                )
                .map(|value| value.into_py(py))
            })
        })
    }
}

#[pyclass(module = "hermes._native")]
struct Receipt {
    receipt: Mutex<Option<EmissionReceipt>>,
}

#[pymethods]
impl Receipt {
    fn wait<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        // Take before awaiting: a cancelled Python task cannot reuse a consumed receipt.
        let receipt = self
            .receipt
            .try_lock()
            .map_err(|_| HermesError::new_err("receipt is already being consumed"))?
            .take()
            .ok_or_else(|| HermesError::new_err("receipt was already consumed"))?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            receipt.wait().await.map_err(core_error)
        })
    }
}

#[pyclass(module = "hermes._native")]
struct Emissions {
    sub: Arc<Mutex<BotEmissionSubscription>>,
}

#[pymethods]
impl Emissions {
    fn next<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let sub = Arc::clone(&self.sub);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let event = sub.lock().await.next().await.map_err(core_error)?;
            Python::with_gil(|py| match event {
                Some(event) => Py::new(
                    py,
                    Emission {
                        bot_id: event.bot_id,
                        emission_type: event.emission_type,
                        data_json: struct_json(event.data),
                    },
                )
                .map(|value| value.into_py(py)),
                None => Ok(py.None()),
            })
        })
    }
}

#[pyclass(module = "hermes._native")]
struct BotHeartbeats {
    sub: Arc<Mutex<BotHeartbeatSubscription>>,
}

#[pymethods]
impl BotHeartbeats {
    fn next<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let sub = Arc::clone(&self.sub);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let event = sub.lock().await.next().await.map_err(core_error)?;
            Python::with_gil(|py| match event {
                Some(event) => {
                    Py::new(py, HeartbeatEvent::from_bot(event)).map(|value| value.into_py(py))
                }
                None => Ok(py.None()),
            })
        })
    }
}

#[pyclass(module = "hermes._native")]
struct WranglerHeartbeats {
    sub: Arc<Mutex<WranglerHeartbeatSubscription>>,
}

#[pymethods]
impl WranglerHeartbeats {
    fn next<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let sub = Arc::clone(&self.sub);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let event = sub.lock().await.next().await.map_err(core_error)?;
            Python::with_gil(|py| match event {
                Some(event) => {
                    Py::new(py, HeartbeatEvent::from_wrangler(event)).map(|value| value.into_py(py))
                }
                None => Ok(py.None()),
            })
        })
    }
}

#[pyclass(module = "hermes._native")]
struct ActiveOrdersRequests {
    sub: Arc<Mutex<ActiveOrdersRequestSubscription>>,
}

#[pymethods]
impl ActiveOrdersRequests {
    fn next<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let sub = Arc::clone(&self.sub);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let incoming = sub.lock().await.next().await.map_err(core_error)?;
            Python::with_gil(|py| match incoming {
                Some(incoming) => Py::new(
                    py,
                    ActiveOrdersRequest {
                        bot_id: incoming.request.bot_id.clone(),
                        incoming: Mutex::new(Some(incoming)),
                    },
                )
                .map(|value| value.into_py(py)),
                None => Ok(py.None()),
            })
        })
    }
}

#[pyclass(module = "hermes._native")]
struct ActiveOrdersRequest {
    #[pyo3(get)]
    bot_id: String,
    incoming: Mutex<Option<IncomingActiveOrdersRequest>>,
}

#[pymethods]
impl ActiveOrdersRequest {
    fn respond<'py>(
        &self,
        py: Python<'py>,
        generation_id: String,
        version: u64,
        as_of_unix_ns: i64,
        orders_json: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let orders = parse_orders(orders_json)?;
        let incoming = self
            .incoming
            .try_lock()
            .map_err(|_| HermesError::new_err("request is already being consumed"))?
            .take()
            .ok_or_else(|| HermesError::new_err("request was already consumed"))?;
        let snapshot = proto::BotActiveOrdersSnapshot {
            bot_id: incoming.request.bot_id.clone(),
            generation_id,
            version,
            as_of_unix_ns,
            orders,
        };
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            incoming.respond(snapshot).await.map_err(core_error)
        })
    }
}

#[pyclass(module = "hermes._native")]
struct OrdersChanged {
    sub: Arc<Mutex<OrdersChangedSubscription>>,
}

#[pymethods]
impl OrdersChanged {
    fn next<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let sub = Arc::clone(&self.sub);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let notification = sub.lock().await.next().await.map_err(core_error)?;
            Python::with_gil(|py| match notification {
                Some(notification) => Py::new(
                    py,
                    OrdersChangedNotification {
                        bot_id: notification.bot_id,
                        generation_id: notification.generation_id,
                        version: notification.version,
                    },
                )
                .map(|value| value.into_py(py)),
                None => Ok(py.None()),
            })
        })
    }
}

#[pyclass(frozen, module = "hermes._native")]
struct ActiveOrdersSnapshot {
    #[pyo3(get)]
    bot_id: String,
    #[pyo3(get)]
    generation_id: String,
    #[pyo3(get)]
    version: u64,
    #[pyo3(get)]
    as_of_unix_ns: i64,
    #[pyo3(get)]
    orders_json: String,
}

impl ActiveOrdersSnapshot {
    fn from_proto(snapshot: proto::BotActiveOrdersSnapshot) -> Self {
        Self {
            bot_id: snapshot.bot_id,
            generation_id: snapshot.generation_id,
            version: snapshot.version,
            as_of_unix_ns: snapshot.as_of_unix_ns,
            orders_json: orders_json(snapshot.orders),
        }
    }
}

#[pyclass(frozen, module = "hermes._native")]
struct OrdersChangedNotification {
    #[pyo3(get)]
    bot_id: String,
    #[pyo3(get)]
    generation_id: String,
    #[pyo3(get)]
    version: u64,
}

#[pyclass(frozen, module = "hermes._native")]
struct HeartbeatEvent {
    #[pyo3(get)]
    source_kind: String,
    #[pyo3(get)]
    source_id: String,
    #[pyo3(get)]
    status: i32,
    #[pyo3(get)]
    started_at: u64,
    #[pyo3(get)]
    paused_at: Option<u64>,
    #[pyo3(get)]
    config_json: Option<String>,
}

impl HeartbeatEvent {
    fn from_bot(bot: proto::BotHeartbeat) -> Self {
        Self {
            source_kind: "bot".to_owned(),
            source_id: bot.bot_id,
            status: bot.status,
            started_at: bot.started_at,
            paused_at: bot.paused_at,
            config_json: Some(struct_json(bot.config_schema)),
        }
    }

    fn from_wrangler(wrangler: proto::WranglerHeartbeat) -> Self {
        Self {
            source_kind: "wrangler".to_owned(),
            source_id: "wrangler".to_owned(),
            status: wrangler.status,
            started_at: wrangler.started_at,
            paused_at: wrangler.paused_at,
            config_json: None,
        }
    }
}

#[pyclass(frozen, module = "hermes._native")]
struct Emission {
    #[pyo3(get)]
    bot_id: String,
    #[pyo3(get)]
    emission_type: String,
    #[pyo3(get)]
    data_json: String,
}

struct BotSessionMutable {
    active: bool,
    status: i32,
    paused_at: Option<u64>,
    config: Struct,
}

struct BotSessionState {
    core: HermesCore,
    bot_id: String,
    started_at: u64,
    heartbeat_interval: Duration,
    mutable: RwLock<BotSessionMutable>,
    callback: StdMutex<Option<Py<PyAny>>>,
    changed: Notify,
    shutdown: Notify,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl BotSessionState {
    fn heartbeat(&self) -> proto::BotHeartbeat {
        let state = self.mutable.read().expect("bot session state poisoned");
        proto::BotHeartbeat {
            bot_id: self.bot_id.clone(),
            status: state.status,
            started_at: self.started_at,
            paused_at: state.paused_at,
            config_schema: Some(state.config.clone()),
        }
    }

    fn is_active(&self) -> bool {
        self.mutable
            .read()
            .expect("bot session state poisoned")
            .active
    }
}

async fn heartbeat_loop(state: Arc<BotSessionState>) {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(state.heartbeat_interval) => {}
            _ = state.shutdown.notified() => break,
        }
        if !state.is_active() {
            break;
        }
        let _ = state.core.publish_bot_heartbeat(state.heartbeat()).await;
    }
}

fn command_response(
    command: &proto::BotCommand,
    success: bool,
    message: Struct,
    error: Option<String>,
) -> proto::BotCommandResponse {
    proto::BotCommandResponse {
        bot_id: command.bot_id.clone(),
        command_id: command.command_id.clone(),
        success,
        message: Some(message),
        error,
    }
}

fn empty_struct() -> Struct {
    Struct::default()
}

async fn callback_result(callback: Py<PyAny>, command: proto::BotCommand) -> PyResult<Struct> {
    let result = Python::with_gil(|py| -> PyResult<CallbackInvocation> {
        let argument = Py::new(
            py,
            BotCommand {
                bot_id: command.bot_id,
                command_id: command.command_id,
                command_type: command.r#type,
                data_json: struct_json(command.data),
            },
        )?;
        let result = callback.call1(py, (argument,))?;
        if result.bind(py).hasattr("__await__")? {
            Ok(CallbackInvocation::Awaitable(result))
        } else {
            Ok(CallbackInvocation::Ready(result))
        }
    })?;
    let value = match result {
        CallbackInvocation::Ready(value) => value,
        CallbackInvocation::Awaitable(awaitable) => {
            let future = Python::with_gil(|py| {
                pyo3_async_runtimes::tokio::into_future(awaitable.bind(py).clone())
            })?;
            future.await?
        }
    };
    Python::with_gil(|py| {
        if value.bind(py).is_none() {
            return Ok(empty_struct());
        }
        let json = if let Ok(value) = value.extract::<String>(py) {
            value
        } else {
            py.import_bound("json")?
                .call_method1("dumps", (value,))?
                .extract::<String>()?
        };
        parse_data(&json)
    })
}

enum CallbackInvocation {
    Ready(Py<PyAny>),
    Awaitable(Py<PyAny>),
}

async fn command_loop(
    state: Arc<BotSessionState>,
    mut subscription: hermes_core::BotCommandSubscription,
) {
    while state.is_active() {
        let incoming = tokio::select! {
            incoming = subscription.next() => incoming,
            _ = state.shutdown.notified() => break,
        };
        let incoming = match incoming {
            Ok(Some(incoming)) => incoming,
            Ok(None) => break,
            Err(_) => continue,
        };
        let command = incoming.command.clone();
        let response = match command.r#type {
            1 => {
                {
                    let mut mutable = state.mutable.write().expect("bot session state poisoned");
                    mutable.status = proto::Status::Paused as i32;
                    mutable.paused_at = None;
                }
                state.changed.notify_waiters();
                command_response(&command, true, empty_struct(), None)
            }
            2 => {
                {
                    let mut mutable = state.mutable.write().expect("bot session state poisoned");
                    mutable.status = proto::Status::Online as i32;
                    mutable.paused_at = None;
                }
                state.changed.notify_waiters();
                command_response(&command, true, empty_struct(), None)
            }
            3 => {
                let config = state
                    .mutable
                    .read()
                    .expect("bot session state poisoned")
                    .config
                    .clone();
                command_response(&command, true, config, None)
            }
            _ => {
                let callback = Python::with_gil(|py| {
                    state
                        .callback
                        .lock()
                        .expect("bot callback lock poisoned")
                        .as_ref()
                        .map(|callback| callback.clone_ref(py))
                });
                match callback {
                    Some(callback) => match callback_result(callback, command.clone()).await {
                        Ok(message) => command_response(&command, true, message, None),
                        Err(error) => command_response(
                            &command,
                            false,
                            empty_struct(),
                            Some(error.to_string()),
                        ),
                    },
                    None => command_response(
                        &command,
                        false,
                        empty_struct(),
                        Some("no command callback registered".to_owned()),
                    ),
                }
            }
        };
        let _ = incoming.respond(response).await;
    }
}

#[pyclass(module = "hermes._native")]
struct BotSession {
    state: Arc<BotSessionState>,
}

impl Drop for BotSession {
    fn drop(&mut self) {
        if let Ok(mut mutable) = self.state.mutable.write() {
            mutable.active = false;
            mutable.status = proto::Status::ShuttingDown as i32;
            mutable.paused_at = None;
        }
        self.state.changed.notify_waiters();
        self.state.shutdown.notify_waiters();
    }
}

#[pymethods]
impl BotSession {
    #[getter]
    fn bot_id(&self) -> &str {
        &self.state.bot_id
    }

    #[getter]
    fn status(&self) -> i32 {
        self.state
            .mutable
            .read()
            .expect("bot session state poisoned")
            .status
    }

    fn is_active(&self) -> bool {
        self.state.is_active()
    }

    fn is_paused(&self) -> bool {
        self.status() == proto::Status::Paused as i32
    }

    fn mark_online(&self) {
        let mut state = self
            .state
            .mutable
            .write()
            .expect("bot session state poisoned");
        state.status = proto::Status::Online as i32;
        state.paused_at = None;
        drop(state);
        self.state.changed.notify_waiters();
    }

    #[pyo3(signature = (paused_at=None))]
    fn pause(&self, paused_at: Option<u64>) {
        let mut state = self
            .state
            .mutable
            .write()
            .expect("bot session state poisoned");
        state.status = proto::Status::Paused as i32;
        state.paused_at = paused_at;
        drop(state);
        self.state.changed.notify_waiters();
    }

    fn unpause(&self) {
        self.mark_online();
    }

    fn set_config(&self, config_json: &str) -> PyResult<()> {
        let config = parse_data(config_json)?;
        self.state
            .mutable
            .write()
            .expect("bot session state poisoned")
            .config = config;
        Ok(())
    }

    fn on_command<'py>(&self, callback: Py<PyAny>, py: Python<'py>) -> Py<PyAny> {
        *self
            .state
            .callback
            .lock()
            .expect("bot callback lock poisoned") = Some(callback.clone_ref(py));
        callback
    }

    fn wait_until_active<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            loop {
                let notified = state.changed.notified();
                let (active, status) = {
                    let mutable = state.mutable.read().expect("bot session state poisoned");
                    (mutable.active, mutable.status)
                };
                if !active || status == proto::Status::Online as i32 {
                    return Ok(());
                }
                notified.await;
            }
        })
    }

    fn stop<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            {
                let mut mutable = state.mutable.write().expect("bot session state poisoned");
                if !mutable.active {
                    return Ok(());
                }
                mutable.active = false;
                mutable.status = proto::Status::ShuttingDown as i32;
                mutable.paused_at = None;
            }
            state.changed.notify_waiters();
            state.shutdown.notify_waiters();
            let _ = state.core.publish_bot_heartbeat(state.heartbeat()).await;
            for task in state.tasks.lock().await.drain(..) {
                task.abort();
                let _ = task.await;
            }
            Ok(())
        })
    }
}

#[pyclass(frozen, module = "hermes._native")]
struct BotCommand {
    #[pyo3(get)]
    bot_id: String,
    #[pyo3(get)]
    command_id: String,
    #[pyo3(get, name = "type")]
    command_type: i32,
    #[pyo3(get)]
    data_json: String,
}

#[pyclass(frozen, module = "hermes._native")]
struct CommandResponse {
    #[pyo3(get)]
    bot_id: String,
    #[pyo3(get)]
    command_id: String,
    #[pyo3(get)]
    success: bool,
    #[pyo3(get)]
    message_json: String,
    #[pyo3(get)]
    error: Option<String>,
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("HermesError", m.py().get_type_bound::<HermesError>())?;
    m.add("QueueFullError", m.py().get_type_bound::<QueueFullError>())?;
    m.add("STATUS_UNSPECIFIED", proto::Status::Unspecified as i32)?;
    m.add("STATUS_SHUTTING_DOWN", proto::Status::ShuttingDown as i32)?;
    m.add("STATUS_WARMING_UP", proto::Status::WarmingUp as i32)?;
    m.add("STATUS_PAUSED", proto::Status::Paused as i32)?;
    m.add("STATUS_ONLINE", proto::Status::Online as i32)?;
    m.add("COMMAND_TYPE_UNSPECIFIED", 0)?;
    m.add("COMMAND_TYPE_PAUSE", 1)?;
    m.add("COMMAND_TYPE_UNPAUSE", 2)?;
    m.add("COMMAND_TYPE_GET_CONFIG", 3)?;
    m.add("COMMAND_TYPE_MODIFY_CONFIG", 4)?;
    m.add_class::<Hermes>()?;
    m.add_class::<BotSession>()?;
    m.add_class::<BotCommand>()?;
    m.add_class::<Receipt>()?;
    m.add_class::<Emissions>()?;
    m.add_class::<BotHeartbeats>()?;
    m.add_class::<WranglerHeartbeats>()?;
    m.add_class::<HeartbeatEvent>()?;
    m.add_class::<Emission>()?;
    m.add_class::<CommandResponse>()?;
    m.add_class::<ActiveOrdersRequests>()?;
    m.add_class::<ActiveOrdersRequest>()?;
    m.add_class::<ActiveOrdersSnapshot>()?;
    m.add_class::<OrdersChanged>()?;
    m.add_class::<OrdersChangedNotification>()?;

    Ok(())
}

#[cfg(test)]
mod tests;
