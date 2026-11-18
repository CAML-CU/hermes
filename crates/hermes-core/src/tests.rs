use super::*;

#[test]
fn command_responses_must_match_both_identifiers() {
    let command = proto::BotCommand {
        bot_id: "bot-1".into(),
        command_id: "req-1".into(),
        ..Default::default()
    };
    let mut response = proto::BotCommandResponse {
        bot_id: command.bot_id.clone(),
        command_id: command.command_id.clone(),
        ..Default::default()
    };
    assert!(validate_response(&command, &response).is_ok());
    response.command_id = "req-2".into();
    assert!(validate_response(&command, &response).is_err());
    response.command_id = command.command_id.clone();
    response.bot_id = "bot-2".into();
    assert!(validate_response(&command, &response).is_err());
}

#[test]
fn exact_subjects() {
    assert_eq!(subject::bot_emissions("bot-1").unwrap(), "hermes.bot.bot-1.emissions");
    assert_eq!(subject::bot_commands("bot_1").unwrap(), "hermes.bot.bot_1.commands");
    assert_eq!(subject::bot_orders_snapshot("bot-1").unwrap(), "hermes.bot.bot-1.orders.snapshot");
    assert_eq!(subject::bot_orders_changed("bot_1").unwrap(), "hermes.bot.bot_1.orders.changed");
    assert_eq!(subject::BOT_HEARTBEATS, "hermes.heartbeats.bots");
    assert_eq!(subject::WRANGLER_HEARTBEATS, "hermes.heartbeats.wranglers");
    assert_ne!(subject::BOT_HEARTBEATS, subject::WRANGLER_HEARTBEATS);
}

#[test]
fn bot_heartbeats_require_id_and_wrangler_is_singleton() {
    assert!(validate_bot_heartbeat(&proto::BotHeartbeat::default()).is_err());
    let bot = proto::BotHeartbeat {
        bot_id: "bot-1".into(),
        ..Default::default()
    };
    assert!(validate_bot_heartbeat(&bot).is_ok());
    let bot_bytes = prost::Message::encode_to_vec(&bot);
    assert_eq!(<proto::BotHeartbeat as prost::Message>::decode(bot_bytes.as_slice()).unwrap(), bot);

    let wrangler = proto::WranglerHeartbeat {
        status: proto::Status::Online as i32,
        ..Default::default()
    };
    let wrangler_bytes = prost::Message::encode_to_vec(&wrangler);
    assert_eq!(<proto::WranglerHeartbeat as prost::Message>::decode(wrangler_bytes.as_slice()).unwrap(), wrangler);
}

#[test]
fn subjects_reject_wildcards_and_injection() {
    for bad in ["", ">", "*", "a.b", "a b", "é", "a\n"] {
        assert!(subject::bot_emissions(bad).is_err(), "{bad:?}");
        assert!(subject::bot_commands(bad).is_err(), "{bad:?}");
        assert!(subject::bot_orders_snapshot(bad).is_err(), "{bad:?}");
        assert!(subject::bot_orders_changed(bad).is_err(), "{bad:?}");
    }
}

#[test]
fn order_snapshots_require_matching_bot_and_versioned_generation() {
    let request = proto::BotActiveOrdersRequest { bot_id: "bot-1".into() };
    let mut snapshot = proto::BotActiveOrdersSnapshot {
        bot_id: request.bot_id.clone(),
        generation_id: "run-1".into(),
        version: 1,
        ..Default::default()
    };
    assert!(validate_snapshot(&request, &snapshot).is_ok());
    let bytes = snapshot.encode_to_vec();
    assert_eq!(proto::BotActiveOrdersSnapshot::decode(bytes.as_slice()).unwrap(), snapshot);
    snapshot.bot_id = "bot-2".into();
    assert!(validate_snapshot(&request, &snapshot).is_err());
    snapshot.bot_id = request.bot_id.clone();
    snapshot.generation_id.clear();
    assert!(validate_snapshot(&request, &snapshot).is_err());
    snapshot.generation_id = "run-1".into();
    snapshot.version = 0;
    assert!(validate_snapshot(&request, &snapshot).is_err());
}

#[test]
fn orders_changed_requires_versioned_generation() {
    let mut notification = proto::BotOrdersChanged {
        bot_id: "bot-1".into(),
        generation_id: "run-1".into(),
        version: 1,
    };
    assert!(validate_orders_changed(&notification).is_ok());
    assert!(validate_received_orders_changed(&notification, "bot-1").is_ok());
    assert!(validate_received_orders_changed(&notification, "bot-2").is_err());
    let bytes = notification.encode_to_vec();
    assert_eq!(proto::BotOrdersChanged::decode(bytes.as_slice()).unwrap(), notification);
    notification.generation_id.clear();
    assert!(validate_orders_changed(&notification).is_err());
    notification.generation_id = "run-1".into();
    notification.version = 0;
    assert!(validate_orders_changed(&notification).is_err());
}

#[test]
fn snapshot_size_limit_is_one_mib_encoded() {
    let snapshot = proto::BotActiveOrdersSnapshot {
        bot_id: "bot-1".into(),
        generation_id: "run-1".into(),
        version: 1,
        orders: vec![proto::ActiveOrder {
            symbol: "x".repeat(MAX_SNAPSHOT_BYTES),
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!(validate_snapshot_size(MAX_SNAPSHOT_BYTES).is_ok());
    assert!(validate_snapshot_size(snapshot.encoded_len()).is_err());
}
