import unittest
from unittest.mock import patch

from tsx import (
    iTSBaseProviderMock,
    iTSmsProviderMock,
    iTSnsProviderMock,
    iTSProviderMock,
    iTSusProviderMock,
    iTSBaseProvider,
    iTS,
    iTSProvider,
    iTSms,
    iTSmsProvider,
    iTSns,
    iTSnsProvider,
    iTSus,
    iTSusProvider,
)
from tsx.ts import dTS, iBaseTS


class TestiTSBaseProvider(unittest.TestCase):
    def test_its_base_provider_is_base_class(self):
        with self.assertRaises(NotImplementedError):
            iTSBaseProvider().now()

        self.assertTrue(issubclass(iTSProvider, iTSBaseProvider))
        self.assertTrue(issubclass(iTSmsProvider, iTSBaseProvider))
        self.assertTrue(issubclass(iTSusProvider, iTSBaseProvider))
        self.assertTrue(issubclass(iTSnsProvider, iTSBaseProvider))

    def test_precision_provider_now_return_types(self):
        self.assertIsInstance(iTSmsProvider().now(), iBaseTS)
        self.assertIsInstance(iTSProvider().now(), iTS)
        self.assertIsInstance(iTSmsProvider().now(), iTSms)
        self.assertIsInstance(iTSusProvider().now(), iTSus)
        self.assertIsInstance(iTSnsProvider().now(), iTSns)

    def test_zero_sleep_is_executed(self):
        provider = iTSmsProvider()

        with patch("tsx.time_provider.time.sleep") as sleep:
            provider.sleep_ns(0)
            provider.sleep_us(0)
            provider.sleep_ms(0)
            provider.sleep_s(0)
            provider.sleep(dTS(0, unit="s"))

        self.assertEqual([call.args[0] for call in sleep.call_args_list], [0.0, 0.0, 0.0, 0, 0.0])

    def test_negative_sleep_is_rejected(self):
        provider = iTSmsProvider()

        for sleep_call in (
            lambda: provider.sleep_ns(-1),
            lambda: provider.sleep_us(-1),
            lambda: provider.sleep_ms(-1),
            lambda: provider.sleep_s(-1),
            lambda: provider.sleep(dTS(-1, unit="ns")),
        ):
            with self.subTest(sleep_call=sleep_call):
                with self.assertRaises(ValueError):
                    sleep_call()

    def test_sleep_delta_uses_scalar_duration(self):
        start = iTSns("2026-07-10T12:00:00Z")
        provider = iTSmsProvider()

        with patch.object(iTSns, "now", return_value=start), patch("tsx.time_provider.time.sleep") as sleep:
            provider.sleep(dTS(1500, unit="ms"))

        sleep.assert_called_once_with(1.5)

    def test_sleep_delta_uses_calendar_duration_from_now(self):
        start = iTSns("2024-01-31T00:00:00Z")
        expected_target = dTS(1, unit="M")._add(int(start))
        expected_sleep_seconds = (expected_target - int(start)) / 1_000_000_000
        provider = iTSmsProvider()

        with patch.object(iTSns, "now", return_value=start), patch("tsx.time_provider.time.sleep") as sleep:
            provider.sleep(dTS(1, unit="M"))

        sleep.assert_called_once_with(expected_sleep_seconds)


class TestiTSBaseProviderMock(unittest.TestCase):
    def test_mock_base_provider_matches_provider_hierarchy(self):
        with self.assertRaises(NotImplementedError):
            iTSBaseProviderMock().now()

        self.assertTrue(issubclass(iTSBaseProviderMock, iTSBaseProvider))
        self.assertTrue(issubclass(iTSProviderMock, iTSBaseProviderMock))
        self.assertTrue(issubclass(iTSmsProviderMock, iTSBaseProviderMock))
        self.assertTrue(issubclass(iTSusProviderMock, iTSBaseProviderMock))
        self.assertTrue(issubclass(iTSnsProviderMock, iTSBaseProviderMock))
        self.assertTrue(issubclass(iTSProviderMock, iTSProvider))
        self.assertTrue(issubclass(iTSmsProviderMock, iTSmsProvider))
        self.assertTrue(issubclass(iTSusProviderMock, iTSusProvider))
        self.assertTrue(issubclass(iTSnsProviderMock, iTSnsProvider))

    def test_instances_have_independent_state(self):
        first = iTSmsProviderMock(iTSms(1000))
        second = iTSmsProviderMock(iTSms(2000))

        first.advance_ms(500)

        self.assertEqual(first.now(), iTSms(1500))
        self.assertEqual(second.now(), iTSms(2000))

    def test_initial_timestamp_conversion(self):
        self.assertEqual(iTSProviderMock(1_500_000_000).now(), iTS(2))
        self.assertEqual(iTSmsProviderMock(1_500_000_000).now(), iTSms(1500))
        self.assertEqual(iTSnsProviderMock(iTSms(1500)).now(), iTSns(1_500_000_000))

    def test_set_ns_and_advance_methods(self):
        clock = iTSnsProviderMock(1_000)

        clock.set_ns(2_000)
        self.assertEqual(clock.now(), iTSns(2_000))

        clock.advance_ns(1)
        self.assertEqual(clock.now(), iTSns(2_001))

        clock.advance_us(2)
        self.assertEqual(clock.now(), iTSns(4_001))

        clock.advance_ms(3)
        self.assertEqual(clock.now(), iTSns(3_004_001))

        clock.advance_s(1.5)
        self.assertEqual(clock.now(), iTSns(1_503_004_001))

    def test_advance_delta_uses_calendar_duration_from_current_mock_time(self):
        clock = iTSnsProviderMock(iTSns("2024-01-31T00:00:00Z"))
        expected = iTSns("2024-02-29T00:00:00Z")

        clock.advance(dTS(1, unit="M"))

        self.assertEqual(clock.now(), expected)

    def test_backward_and_non_positive_advances_are_rejected(self):
        clock = iTSnsProviderMock(100)

        for advance_call in (
            lambda: clock.set_ns(99),
            lambda: clock.advance_ns(0),
            lambda: clock.advance_us(0),
            lambda: clock.advance_ms(0),
            lambda: clock.advance_s(0),
            lambda: clock.advance(dTS(0, unit="ns")),
        ):
            with self.subTest(advance_call=advance_call):
                with self.assertRaises(ValueError):
                    advance_call()

    def test_sleep_methods_advance_mock_time(self):
        clock = iTSnsProviderMock(1_000)

        clock.sleep_ns(1)
        clock.sleep_us(2)
        clock.sleep_ms(3)
        clock.sleep_s(1)
        clock.sleep(dTS(4, unit="ms"))

        self.assertEqual(clock.now(), iTSns(1_007_003_001))

    def test_negative_mock_sleep_is_rejected_and_zero_is_noop(self):
        clock = iTSnsProviderMock(1_000)

        clock.sleep_ns(0)
        clock.sleep_us(0)
        clock.sleep_ms(0)
        clock.sleep_s(0)
        clock.sleep(dTS(0, unit="ns"))
        self.assertEqual(clock.now(), iTSns(1_000))

        for sleep_call in (
            lambda: clock.sleep_ns(-1),
            lambda: clock.sleep_us(-1),
            lambda: clock.sleep_ms(-1),
            lambda: clock.sleep_s(-1),
            lambda: clock.sleep(dTS(-1, unit="ns")),
        ):
            with self.subTest(sleep_call=sleep_call):
                with self.assertRaises(ValueError):
                    sleep_call()

    def test_immediate_callback_executes_without_moving_time_backwards(self):
        clock = iTSnsProviderMock(10)
        calls = []

        clock.wait_and_execute_ns(5, lambda: calls.append(int(clock.now())))

        self.assertEqual(calls, [10])
        self.assertEqual(clock.now(), iTSns(10))

    def test_delayed_callbacks_run_at_target_time_in_order(self):
        clock = iTSnsProviderMock(0)
        calls = []

        clock.wait_and_execute_ns(30, lambda: calls.append(("third", int(clock.now()))))
        clock.wait_and_execute_ns(10, lambda: calls.append(("first", int(clock.now()))))
        clock.wait_and_execute_ns(20, lambda: calls.append(("second", int(clock.now()))))

        clock.advance_ns(30)

        self.assertEqual(calls, [("first", 10), ("second", 20), ("third", 30)])
        self.assertEqual(clock.now(), iTSns(30))

    def test_equal_target_callbacks_keep_insertion_order(self):
        clock = iTSnsProviderMock(0)
        calls = []

        clock.wait_and_execute_ns(10, lambda: calls.append("first"))
        clock.wait_and_execute_ns(10, lambda: calls.append("second"))

        clock.advance_ns(10)

        self.assertEqual(calls, ["first", "second"])

    def test_callbacks_scheduled_by_callbacks_are_executed_during_same_advance(self):
        clock = iTSnsProviderMock(0)
        calls = []

        def first():
            calls.append(("first", int(clock.now())))
            clock.wait_and_execute_ns(15, lambda: calls.append(("second", int(clock.now()))))

        clock.wait_and_execute_ns(10, first)
        clock.advance_ns(20)

        self.assertEqual(calls, [("first", 10), ("second", 15)])
        self.assertEqual(clock.now(), iTSns(20))

    def test_callback_exception_propagates_and_leaves_time_at_operation_target(self):
        clock = iTSnsProviderMock(0)

        def fail():
            raise RuntimeError("boom")

        clock.wait_and_execute_ns(10, fail)

        with self.assertRaisesRegex(RuntimeError, "boom"):
            clock.advance_ns(20)

        self.assertEqual(clock.now(), iTSns(10))


if __name__ == "__main__":
    unittest.main()
