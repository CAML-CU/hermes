import unittest

import hermes


class PythonBindingTests(unittest.TestCase):
    def test_public_api_is_exported(self) -> None:
        expected = {
            "Hermes",
            "BotSession",
            "BotCommand",
            "HermesError",
            "QueueFullError",
            "Receipt",
            "Emission",
            "HeartbeatEvent",
            "CommandResponse",
            "ActiveOrdersSnapshot",
            "OrdersChangedNotification",
            "STATUS_ONLINE",
            "COMMAND_TYPE_PAUSE",
        }
        self.assertTrue(expected.issubset(hermes.__all__))
        for name in hermes.__all__:
            self.assertIsNotNone(getattr(hermes, name))

    def test_errors_have_public_hierarchy(self) -> None:
        self.assertTrue(issubclass(hermes.HermesError, RuntimeError))
        self.assertTrue(issubclass(hermes.QueueFullError, hermes.HermesError))

    def test_native_types_live_in_private_module(self) -> None:
        for native_type in (
            hermes.Hermes,
            hermes.BotSession,
            hermes.BotCommand,
            hermes.Receipt,
            hermes.Emission,
            hermes.HeartbeatEvent,
            hermes.CommandResponse,
        ):
            self.assertEqual(native_type.__module__, "hermes._native")

    def test_protocol_constants_are_exposed(self) -> None:
        self.assertEqual(hermes.STATUS_UNSPECIFIED, 0)
        self.assertEqual(hermes.STATUS_ONLINE, 4)
        self.assertEqual(hermes.COMMAND_TYPE_UNSPECIFIED, 0)
        self.assertEqual(hermes.COMMAND_TYPE_PAUSE, 1)
        self.assertEqual(hermes.COMMAND_TYPE_MODIFY_CONFIG, 4)


if __name__ == "__main__":
    unittest.main()
