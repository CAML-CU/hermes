use super::*;

#[test]
fn orders_round_trip_preserves_decimal_strings() {
    let input = r#"[{"venue":"X","symbol":"BTC/USD","order_id":"o1","client_order_id":"c1","side":"buy","status":"open","limit_price":"12345678901234567890.00000000000000000001","total_quantity":"0.00000000000000000001","filled_quantity":"0"},{"venue":"Y","symbol":"ETH/USD","order_id":"o2","client_order_id":"c2","side":"sell","status":"pending","total_quantity":"3","filled_quantity":"0"}]"#;
    let parsed = parse_orders(input).unwrap();
    assert_eq!(parsed[0].limit_price.as_deref(), Some("12345678901234567890.00000000000000000001"));
    assert_eq!(parsed[1].limit_price, None);
    let output = orders_json(parsed);
    let values: serde_json::Value = serde_json::from_str(&output).unwrap();
    assert_eq!(values[0]["total_quantity"], "0.00000000000000000001");
    assert_eq!(values[0]["limit_price"], "12345678901234567890.00000000000000000001");
    assert!(values[1]["limit_price"].is_null());
    assert_eq!(parse_orders(&output).unwrap().len(), 2);
}

#[test]
fn orders_reject_invalid_shapes_and_numeric_decimals() {
    let valid = r#"{"venue":"X","symbol":"BTC/USD","order_id":"o","client_order_id":"c","side":"buy","status":"open","limit_price":"1.2","total_quantity":"2","filled_quantity":"0"}"#;
    assert!(parse_orders("[]").unwrap().is_empty());
    for invalid in [
        "{}", "null", "not json", "[null]", "[1]",
        &format!("[{valid},{{}}]"),
        &format!("[{}]", valid.replace("\"total_quantity\":\"2\"", "\"total_quantity\":2.0")),
        &format!("[{}]", valid.replace("\"filled_quantity\":\"0\"", "\"filled_quantity\":0")),
        &format!("[{}]", valid.replace("\"limit_price\":\"1.2\"", "\"limit_price\":1.2")),
        &format!("[{}]", valid.replace("\"venue\":\"X\",", "")),
        &format!("[{}]", valid.replace("\"venue\":\"X\"", "\"venue\":\"X\",\"extra\":1")),
    ] {
        assert!(parse_orders(invalid).is_err(), "accepted {invalid}");
    }
}

#[test]
fn snapshot_preserves_metadata_and_order_json() {
    let snapshot = ActiveOrdersSnapshot::from_proto(proto::BotActiveOrdersSnapshot {
        bot_id: "bot-1".into(), generation_id: "generation-2".into(),
        version: u64::MAX, as_of_unix_ns: -123, orders: parse_orders("[]").unwrap(),
    });
    assert_eq!(snapshot.bot_id, "bot-1");
    assert_eq!(snapshot.generation_id, "generation-2");
    assert_eq!(snapshot.version, u64::MAX);
    assert_eq!(snapshot.as_of_unix_ns, -123);
    assert_eq!(snapshot.orders_json, "[]");
}

#[test]
fn struct_json_round_trip() {
    let input = r#"{"name":"bot","nested":{"enabled":true},"items":[null,1.5]}"#;
    let data = parse_data(input).unwrap();
    let result: serde_json::Value = serde_json::from_str(&struct_json(Some(data))).unwrap();
    assert_eq!(result, serde_json::from_str::<serde_json::Value>(input).unwrap());
}

#[test]
fn rejects_non_object_payloads() {
    assert!(parse_data("[]").is_err());
    assert!(parse_data("not json").is_err());
}

#[test]
fn heartbeat_config_must_be_an_object() {
    for invalid in ["[]", "null", "3", "\"text\"", "not json"] {
        assert!(parse_data(invalid).is_err(), "accepted {invalid}");
    }
    let config = struct_json(Some(parse_data(r#"{"risk":{"limit":2}}"#).unwrap()));
    assert_eq!(serde_json::from_str::<serde_json::Value>(&config).unwrap()["risk"]["limit"], 2.0);
}

#[test]
fn bot_heartbeat_event_preserves_fields() {
    let event = HeartbeatEvent::from_bot(proto::BotHeartbeat {
        bot_id: "bot-1".into(),
        status: proto::Status::Paused as i32,
        started_at: 123,
        paused_at: Some(456),
        config_schema: Some(parse_data(r#"{"mode":"safe"}"#).unwrap()),
    });
    assert_eq!(event.source_kind, "bot");
    assert_eq!(event.source_id, "bot-1");
    assert_eq!(event.status, proto::Status::Paused as i32);
    assert_eq!(event.started_at, 123);
    assert_eq!(event.paused_at, Some(456));
    assert_eq!(event.config_json.as_deref(), Some(r#"{"mode":"safe"}"#));
}

#[test]
fn wrangler_heartbeat_event_preserves_fields() {
    let event = HeartbeatEvent::from_wrangler(proto::WranglerHeartbeat {

        status: proto::Status::Online as i32,
        started_at: 789,
        paused_at: None,
    });
    assert_eq!(event.source_kind, "wrangler");
    assert_eq!(event.source_id, "wrangler");
    assert_eq!(event.status, proto::Status::Online as i32);
    assert_eq!(event.started_at, 789);
    assert_eq!(event.paused_at, None);
    assert_eq!(event.config_json, None);
}


#[test]
fn bot_heartbeat_without_config_uses_empty_object() {
    let event = HeartbeatEvent::from_bot(proto::BotHeartbeat {
        bot_id: "bot-1".into(),
        status: 0,
        started_at: 0,
        paused_at: None,
        config_schema: None,
    });
    assert_eq!(event.config_json.as_deref(), Some("{}"));
}
