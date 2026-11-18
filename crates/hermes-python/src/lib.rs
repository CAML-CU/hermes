use std::{sync::Arc, time::Duration};

use hermes_core::{proto, ActiveOrdersRequestSubscription, BotEmissionSubscription, BotHeartbeatSubscription, EmissionReceipt, HermesCore, IncomingActiveOrdersRequest, OrdersChangedSubscription, WranglerHeartbeatSubscription};
use prost_types::{value::Kind, ListValue, Struct, Value};
use pyo3::{create_exception, exceptions::{PyRuntimeError, PyValueError}, prelude::*};
use serde::Deserialize;
use tokio::sync::Mutex;

create_exception!(hermes_python, HermesError, PyRuntimeError);
create_exception!(hermes_python, QueueFullError, HermesError);

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
            fields: fields.into_iter().map(|(key, value)| (key, json_value(value))).collect(),
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
        Some(Kind::ListValue(list)) => serde_json::Value::Array(list.values.into_iter().map(to_json).collect()),
        Some(Kind::StructValue(data)) => serde_json::Value::Object(
            data.fields.into_iter().map(|(key, value)| (key, to_json(value))).collect(),
        ),
    }
}

fn struct_json(data: Option<Struct>) -> String {
    let value = data.map(|data| to_json(Value { kind: Some(Kind::StructValue(data)) }))
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
    Ok(orders.into_iter().map(|order| proto::ActiveOrder {
        venue: order.venue,
        symbol: order.symbol,
        order_id: order.order_id,
        client_order_id: order.client_order_id,
        side: order.side,
        status: order.status,
        limit_price: order.limit_price,
        total_quantity: order.total_quantity,
        filled_quantity: order.filled_quantity,
    }).collect())
}

fn orders_json(orders: Vec<proto::ActiveOrder>) -> String {
    serde_json::Value::Array(orders.into_iter().map(|order| serde_json::json!({
        "venue": order.venue,
        "symbol": order.symbol,
        "order_id": order.order_id,
        "client_order_id": order.client_order_id,
        "side": order.side,
        "status": order.status,
        "limit_price": order.limit_price,
        "total_quantity": order.total_quantity,
        "filled_quantity": order.filled_quantity,
    })).collect()).to_string()
}

#[pyclass]
struct Hermes {
    core: HermesCore,
}

#[pymethods]
impl Hermes {
    #[staticmethod]
    #[pyo3(signature = (url, emission_capacity=1024))]
    fn connect<'py>(py: Python<'py>, url: String, emission_capacity: usize) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let core = HermesCore::connect(&url, emission_capacity).await.map_err(core_error)?;
            Python::with_gil(|py| Py::new(py, Self { core }).map(|value| value.into_py(py)))
        })
    }

    #[pyo3(signature = (bot_id, emission_type, data_json="{}"))]
    fn try_emit(&self, py: Python<'_>, bot_id: String, emission_type: String, data_json: &str) -> PyResult<Receipt> {
        let data = parse_data(data_json)?;
        let core = self.core.clone();
        let receipt = py.allow_threads(move || core.try_publish_emission(proto::BotEmission {
            bot_id,
            emission_type,
            data: Some(data),
        })).map_err(core_error)?;
        Ok(Receipt { receipt: Mutex::new(Some(receipt)) })
    }

    #[pyo3(signature = (bot_id, status, started_at, paused_at=None, config_json="{}"))]
    fn publish_bot_heartbeat<'py>(&self, py: Python<'py>, bot_id: String, status: i32, started_at: u64, paused_at: Option<u64>, config_json: &str) -> PyResult<Bound<'py, PyAny>> {
        let config_schema = Some(parse_data(config_json)?);
        let core = self.core.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core.publish_bot_heartbeat(proto::BotHeartbeat {
                bot_id, status, started_at, paused_at, config_schema,
            }).await.map_err(core_error)
        })
    }

    #[pyo3(signature = (status, started_at, paused_at=None))]
    fn publish_wrangler_heartbeat<'py>(&self, py: Python<'py>, status: i32, started_at: u64, paused_at: Option<u64>) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core.publish_wrangler_heartbeat(proto::WranglerHeartbeat {
                status, started_at, paused_at,
            }).await.map_err(core_error)
        })
    }

    fn subscribe_bot_heartbeats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let sub = core.subscribe_bot_heartbeats().await.map_err(core_error)?;
            Python::with_gil(|py| Py::new(py, BotHeartbeats { sub: Arc::new(Mutex::new(sub)) }).map(|value| value.into_py(py)))
        })
    }

    fn subscribe_wrangler_heartbeats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let sub = core.subscribe_wrangler_heartbeats().await.map_err(core_error)?;
            Python::with_gil(|py| Py::new(py, WranglerHeartbeats { sub: Arc::new(Mutex::new(sub)) }).map(|value| value.into_py(py)))
        })
    }

    fn subscribe_bot_emissions<'py>(&self, py: Python<'py>, bot_id: String) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let sub = core.subscribe_bot_emissions(&bot_id).await.map_err(core_error)?;
            Python::with_gil(|py| Py::new(py, Emissions { sub: Arc::new(Mutex::new(sub)) }).map(|value| value.into_py(py)))
        })
    }

    #[pyo3(signature = (bot_id, command_type, data_json="{}", command_id="", timeout_seconds=5.0))]
    fn request_bot_command<'py>(&self, py: Python<'py>, bot_id: String, command_type: i32, data_json: &str, command_id: String, timeout_seconds: f64) -> PyResult<Bound<'py, PyAny>> {
        let data = parse_data(data_json)?;
        if !timeout_seconds.is_finite() || timeout_seconds <= 0.0 {
            return Err(PyValueError::new_err("timeout_seconds must be finite and positive"));
        }
        let timeout = Duration::try_from_secs_f64(timeout_seconds)
            .map_err(|_| PyValueError::new_err("timeout_seconds is too large"))?;
        let core = self.core.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = core.request_bot_command(proto::BotCommand {
                bot_id, r#type: command_type, data: Some(data), command_id,
            }, timeout).await.map_err(core_error)?;
            Python::with_gil(|py| Py::new(py, CommandResponse {
                bot_id: response.bot_id,
                command_id: response.command_id,
                success: response.success,
                message_json: struct_json(response.message),
                error: response.error,
            }).map(|value| value.into_py(py)))
        })
    }

    #[pyo3(signature = (bot_id, timeout_seconds=5.0))]
    fn request_active_orders<'py>(&self, py: Python<'py>, bot_id: String, timeout_seconds: f64) -> PyResult<Bound<'py, PyAny>> {
        if !timeout_seconds.is_finite() || timeout_seconds <= 0.0 {
            return Err(PyValueError::new_err("timeout_seconds must be finite and positive"));
        }
        let timeout = Duration::try_from_secs_f64(timeout_seconds)
            .map_err(|_| PyValueError::new_err("timeout_seconds is too large"))?;
        let core = self.core.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let snapshot = core.request_active_orders(&bot_id, timeout).await.map_err(core_error)?;
            Python::with_gil(|py| Py::new(py, ActiveOrdersSnapshot::from_proto(snapshot)).map(|value| value.into_py(py)))
        })
    }

    fn subscribe_active_orders_requests<'py>(&self, py: Python<'py>, bot_id: String) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let sub = core.subscribe_active_orders_requests(&bot_id).await.map_err(core_error)?;
            Python::with_gil(|py| Py::new(py, ActiveOrdersRequests { sub: Arc::new(Mutex::new(sub)) }).map(|value| value.into_py(py)))
        })
    }

    fn publish_orders_changed<'py>(&self, py: Python<'py>, bot_id: String, generation_id: String, version: u64) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core.publish_orders_changed(proto::BotOrdersChanged { bot_id, generation_id, version }).await.map_err(core_error)
        })
    }

    fn subscribe_orders_changed<'py>(&self, py: Python<'py>, bot_id: String) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let sub = core.subscribe_orders_changed(&bot_id).await.map_err(core_error)?;
            Python::with_gil(|py| Py::new(py, OrdersChanged { sub: Arc::new(Mutex::new(sub)) }).map(|value| value.into_py(py)))
        })
    }
}

#[pyclass]
struct Receipt {
    receipt: Mutex<Option<EmissionReceipt>>,
}

#[pymethods]
impl Receipt {
    fn wait<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        // Take before awaiting: a cancelled Python task cannot reuse a consumed receipt.
        let receipt = self.receipt.try_lock()
            .map_err(|_| HermesError::new_err("receipt is already being consumed"))?
            .take().ok_or_else(|| HermesError::new_err("receipt was already consumed"))?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            receipt.wait().await.map_err(core_error)
        })
    }
}

#[pyclass]
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
                Some(event) => Py::new(py, Emission {
                    bot_id: event.bot_id,
                    emission_type: event.emission_type,
                    data_json: struct_json(event.data),
                }).map(|value| value.into_py(py)),
                None => Ok(py.None()),
            })
        })
    }
}

#[pyclass]
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
                Some(event) => Py::new(py, HeartbeatEvent::from_bot(event)).map(|value| value.into_py(py)),
                None => Ok(py.None()),
            })
        })
    }
}

#[pyclass]
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
                Some(event) => Py::new(py, HeartbeatEvent::from_wrangler(event)).map(|value| value.into_py(py)),
                None => Ok(py.None()),
            })
        })
    }
}

#[pyclass]
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
                Some(incoming) => Py::new(py, ActiveOrdersRequest {
                    bot_id: incoming.request.bot_id.clone(),
                    incoming: Mutex::new(Some(incoming)),
                }).map(|value| value.into_py(py)),
                None => Ok(py.None()),
            })
        })
    }
}

#[pyclass]
struct ActiveOrdersRequest {
    #[pyo3(get)] bot_id: String,
    incoming: Mutex<Option<IncomingActiveOrdersRequest>>,
}

#[pymethods]
impl ActiveOrdersRequest {
    fn respond<'py>(&self, py: Python<'py>, generation_id: String, version: u64, as_of_unix_ns: i64, orders_json: &str) -> PyResult<Bound<'py, PyAny>> {
        let orders = parse_orders(orders_json)?;
        let incoming = self.incoming.try_lock()
            .map_err(|_| HermesError::new_err("request is already being consumed"))?
            .take().ok_or_else(|| HermesError::new_err("request was already consumed"))?;
        let snapshot = proto::BotActiveOrdersSnapshot {
            bot_id: incoming.request.bot_id.clone(), generation_id, version, as_of_unix_ns, orders,
        };
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            incoming.respond(snapshot).await.map_err(core_error)
        })
    }
}

#[pyclass]
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
                Some(notification) => Py::new(py, OrdersChangedNotification {
                    bot_id: notification.bot_id,
                    generation_id: notification.generation_id,
                    version: notification.version,
                }).map(|value| value.into_py(py)),
                None => Ok(py.None()),
            })
        })
    }
}

#[pyclass(frozen)]
struct ActiveOrdersSnapshot {
    #[pyo3(get)] bot_id: String,
    #[pyo3(get)] generation_id: String,
    #[pyo3(get)] version: u64,
    #[pyo3(get)] as_of_unix_ns: i64,
    #[pyo3(get)] orders_json: String,
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

#[pyclass(frozen)]
struct OrdersChangedNotification {
    #[pyo3(get)] bot_id: String,
    #[pyo3(get)] generation_id: String,
    #[pyo3(get)] version: u64,
}

#[pyclass(frozen)]
struct HeartbeatEvent {
    #[pyo3(get)] source_kind: String,
    #[pyo3(get)] source_id: String,
    #[pyo3(get)] status: i32,
    #[pyo3(get)] started_at: u64,
    #[pyo3(get)] paused_at: Option<u64>,
    #[pyo3(get)] config_json: Option<String>,
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

#[pyclass(frozen)]
struct Emission {
    #[pyo3(get)] bot_id: String,
    #[pyo3(get)] emission_type: String,
    #[pyo3(get)] data_json: String,
}

#[pyclass(frozen)]
struct CommandResponse {
    #[pyo3(get)] bot_id: String,
    #[pyo3(get)] command_id: String,
    #[pyo3(get)] success: bool,
    #[pyo3(get)] message_json: String,
    #[pyo3(get)] error: Option<String>,
}


#[pymodule]
fn hermes_python(m: &Bound<'_, PyModule>) -> PyResult<()> {
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
