use hermes_core::{proto, HermesCore};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(run())
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let client = HermesCore::connect("nats://127.0.0.1:4222", 1024).await?;

    // NATS Core is ephemeral, so subscribe before publishing.
    let mut emissions = client.subscribe_bot_emissions("example-bot").await?;

    let receipt = client.try_publish_emission(proto::BotEmission {
        bot_id: "example-bot".to_owned(),
        emission_type: "strategy_signal".to_owned(),
        data: None,
    })?;
    receipt.wait().await?;

    if let Some(emission) = emissions.next().await? {
        println!("{}: {}", emission.bot_id, emission.emission_type);
    }

    Ok(())
}
