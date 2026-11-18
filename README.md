# Hermes

Hermes is a Cargo workspace with two members: `crates/hermes-core` owns the Rust NATS Core transport and Prost-generated protobuf types; `crates/hermes-python` exposes a PyO3 API over that core. Python never connects to NATS or uses a protobuf runtime. Hermes transports control, observability, and current order state, **never trades**. The `.proto` definitions live in `proto/trading/v1/`; `services.proto` does not generate gRPC clients or servers here.

## Build and validate

Install a Rust toolchain, `protoc`, and Buf. From the repository root:

```sh
buf lint
buf build
cargo test --workspace
```

Unit tests live in `src/tests.rs` in each workspace member. To install the Python extension, install Maturin in a Python virtual environment and run `maturin develop` from `crates/hermes-python` (or `python -m pip install .` there). Running the transport requires a NATS server; no JetStream is used.

## NATS subjects

| Subject | Payload | Pattern |
| --- | --- | --- |
| `hermes.bot.<bot_id>.emissions` | `BotEmission` | Publish/subscribe |
| `hermes.bot.<bot_id>.commands` | `BotCommand` → `BotCommandResponse` | Request/reply |
| `hermes.bot.<bot_id>.orders.snapshot` | `BotActiveOrdersRequest` → `BotActiveOrdersSnapshot` | Request/reply |
| `hermes.bot.<bot_id>.orders.changed` | `BotOrdersChanged` | Publish/subscribe |
| `hermes.heartbeats.bots` | `BotHeartbeat` | Publish/subscribe |
| `hermes.heartbeats.wranglers` | `WranglerHeartbeat` | Publish/subscribe |

Bot IDs in subjects must be nonempty and contain only ASCII letters, digits, `_`, or `-`. Bot heartbeats include a bot ID; the singleton wrangler heartbeat needs no instance ID. Publishers and subscribers use separate heartbeat topics. Callers schedule heartbeats; consumers detect liveness using receipt time and their own expiry policy.

**Core NATS is ephemeral:** subscribe before publishing, and refetch state after reconnecting. A successful publish means the client accepted the message, not that any subscriber processed it. Command requests use caller-supplied IDs (or an auto-generated UUID); handlers echo the bot and command IDs. Request timeouts do not cancel execution, and retries can execute commands twice unless handlers deduplicate them.

Bot emissions may be frequent. `HermesCore::try_publish_emission` (Python: `try_emit`) enqueues without waiting for NATS and reports `QueueFull` (`QueueFullError` in Python) when its bounded queue fills. Its optional receipt can be awaited for publish errors, **not** delivery acknowledgments. The bound applies to queued message count, not total bytes or NATS's internal buffers. Use one connected client per producer to share that queue.

## Python API

The extension exposes async connection, heartbeat publishing/subscriptions, command requests, order snapshots, and notifications. Python does not import a NATS client. For example:

```python
import asyncio
import hermes_python as hermes

async def main():
    client = await hermes.Hermes.connect("nats://127.0.0.1:4222", emission_capacity=4096)

    emissions = await client.subscribe_bot_emissions("bot-1")
    receipt = client.try_emit("bot-1", "risk_alert", '{"reason":"exposure_limit"}')
    await receipt.wait()  # Optional; not a subscriber acknowledgment.
    event = await emissions.next()  # None when the subscription ends.

    bot_heartbeats = await client.subscribe_bot_heartbeats()
    wrangler_heartbeats = await client.subscribe_wrangler_heartbeats()
    await client.publish_bot_heartbeat("bot-1", hermes.STATUS_ONLINE, started_at=1730000000,
                                       config_json='{"strategy":"example"}')
    await client.publish_wrangler_heartbeat(hermes.STATUS_ONLINE, started_at=1730000000)
    heartbeat = await wrangler_heartbeats.next()

asyncio.run(main())
```

`BotHeartbeats.next()` and `WranglerHeartbeats.next()` return read-only `HeartbeatEvent` values with `source_kind`, `source_id`, `status`, `started_at`, `paused_at`, and `config_json`. Wrangler events have the constant `source_id` `"wrangler"` and no config; bot events carry their bot ID and config JSON (`"{}"` when absent). The time fields are unsigned integer values supplied by the caller; the protocol does not currently enforce their units. Bot heartbeat config and other `data_json` inputs must be JSON objects. `request_bot_command(bot_id, command_type, data_json="{}", command_id="", timeout_seconds=5.0)` returns a response with a matching command ID; `COMMAND_TYPE_*` and `STATUS_*` constants are available from the extension.

## Active orders

The **bot's order store**, not Hermes, is authoritative. A bot serves `subscribe_active_orders_requests(bot_id)`; each incoming request's async `respond(generation_id, version, as_of_unix_ns, orders_json)` sends its current snapshot. Clients call `request_active_orders(bot_id, timeout_seconds=5.0)` and may subscribe to `subscribe_orders_changed(bot_id)`. The bot publishes `publish_orders_changed(bot_id, generation_id, version)` when the state changes. Subscribe to notifications first, request a snapshot, and refetch if a newer version or generation appears; refetch after reconnecting because notifications are not retained.

Snapshots and notifications require a nonempty `generation_id` and positive `version`. Increment the version for each change within a generation and use a new generation ID after restarting the bot. Snapshot replies must match the requested bot ID and fit within **1 MiB of encoded protobuf**. An oversized snapshot cannot be sent and its requester may time out.

In Python, `orders_json` is an array of order objects with string `venue`, `symbol`, `order_id`, `client_order_id`, `side`, `status`, `total_quantity`, and `filled_quantity`, plus nullable string `limit_price`. Decimal prices and quantities are **strings**, not JSON numbers, so no floating-point conversion is required. Invalid shapes or fields raise `ValueError` before consuming a request. `ActiveOrdersSnapshot` exposes `bot_id`, `generation_id`, `version`, `as_of_unix_ns`, and `orders_json`; `OrdersChangedNotification` exposes `bot_id`, `generation_id`, and `version`. Notifications are refresh hints, not an order history. A request can be answered only once, and cancelling a response does not make it reusable.
