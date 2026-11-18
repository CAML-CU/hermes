use crate::Error;

/// Subject layout (all IDs are single NATS tokens, never wildcards):
/// `hermes.bot.<bot_id>.emissions`, `hermes.bot.<bot_id>.commands`,
/// `hermes.bot.<bot_id>.orders.snapshot`, `hermes.bot.<bot_id>.orders.changed`,
/// `hermes.heartbeats.bots`, and `hermes.heartbeats.wranglers`.
pub const BOT_HEARTBEATS: &str = "hermes.heartbeats.bots";
pub const WRANGLER_HEARTBEATS: &str = "hermes.heartbeats.wranglers";
fn token(value: &str) -> Result<&str, Error> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(Error::InvalidSubjectToken(value.to_owned()));
    }
    Ok(value)
}

pub fn bot_emissions(bot_id: &str) -> Result<String, Error> {
    Ok(format!("hermes.bot.{}.emissions", token(bot_id)?))
}

pub fn bot_commands(bot_id: &str) -> Result<String, Error> {
    Ok(format!("hermes.bot.{}.commands", token(bot_id)?))
}

pub fn bot_orders_snapshot(bot_id: &str) -> Result<String, Error> {
    Ok(format!("hermes.bot.{}.orders.snapshot", token(bot_id)?))
}

pub fn bot_orders_changed(bot_id: &str) -> Result<String, Error> {
    Ok(format!("hermes.bot.{}.orders.changed", token(bot_id)?))
}
