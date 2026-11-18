"""Publish and receive one Hermes bot emission.

Prerequisites:
    maturin develop
    nats-server

Run from the repository root with:
    python examples/basic.py
"""

import asyncio

import hermes


async def main() -> None:
    client = await hermes.Hermes.connect(
        "nats://127.0.0.1:4222",
        emission_capacity=1024,
    )

    # NATS Core is ephemeral, so subscribe before publishing.
    emissions = await client.subscribe_bot_emissions("example-bot")

    receipt = client.try_emit(
        "example-bot",
        "strategy_signal",
        '{"symbol":"BTC-USD","side":"buy","confidence":0.82}',
    )
    await receipt.wait()

    emission = await emissions.next()
    if emission is not None:
        print(
            f"{emission.bot_id}: {emission.emission_type} "
            f"{emission.data_json}"
        )


if __name__ == "__main__":
    asyncio.run(main())
