#!/usr/bin/env python
# coding:utf-8

import heapq
import time
from collections.abc import Callable

from .ts import dTS, iBaseTS, iTS, iTSms, iTSns, iTSus


class iTSBaseProvider:
    @staticmethod
    def _raise_if_negative(duration: int | float, name: str) -> None:
        if duration < 0:
            raise ValueError(f"{name} must not be negative")

    @staticmethod
    def __target_ns_from_delta(start_ns: iTSns, delta: dTS) -> iTSns:
        return iTSns(delta._add(int(start_ns)))

    def sleep(self, delta: dTS) -> None:
        start_ns = iTSns.now()
        target_ns = self.__target_ns_from_delta(start_ns, delta)
        self.sleep_ns(int(target_ns) - int(start_ns))

    def sleep_ns(self, ns: int) -> None:
        self._raise_if_negative(ns, "ns")
        time.sleep(ns / 1_000_000_000)

    def sleep_us(self, us: int) -> None:
        self._raise_if_negative(us, "us")
        self.sleep_ns(us * 1_000)

    def sleep_ms(self, ms: int) -> None:
        self._raise_if_negative(ms, "ms")
        self.sleep_ns(ms * 1_000_000)

    def sleep_s(self, seconds: int | float) -> None:
        self._raise_if_negative(seconds, "seconds")
        time.sleep(seconds)

    def now(self) -> iBaseTS:
        raise NotImplementedError("iTSBaseProvider is a base class; use a concrete provider")


class iTSProvider(iTSBaseProvider):
    def now(self) -> iTS:
        return iTS.now()


class iTSmsProvider(iTSBaseProvider):
    def now(self) -> iTSms:
        return iTSms.now()


class iTSusProvider(iTSBaseProvider):
    def now(self) -> iTSus:
        return iTSus.now()


class iTSnsProvider(iTSBaseProvider):
    def now(self) -> iTSns:
        return iTSns.now()


class iTSBaseProviderMock(iTSBaseProvider):
    @staticmethod
    def _raise_if_non_positive(duration: int | float, name: str) -> None:
        if duration <= 0:
            raise ValueError(f"{name} must be positive")

    @staticmethod
    def __target_ns_from_delta(start_ns: iTSns, delta: dTS) -> iTSns:
        return iTSns(delta._add(int(start_ns)))

    @staticmethod
    def __to_ns(ts: iBaseTS | int | None) -> iTSns:
        if ts is None:
            return iTSns(0)
        if isinstance(ts, iBaseTS):
            return ts.as_nsec()
        return iTSns(ts)

    def __init__(self, ts: iBaseTS | int | None = None) -> None:
        self._now_ns = self.__to_ns(ts)
        self._sequence = 0
        self._operations: list[tuple[int, int, Callable[[], None]]] = []

    def set_ns(self, ns: int) -> None:
        if ns < self._now_ns:
            raise ValueError("ns must not move time backwards")
        self.__move_to_ns(iTSns(ns))

    def advance(self, delta: dTS) -> None:
        target_ns = self.__target_ns_from_delta(self._now_ns, delta)
        self.__advance_to_ns(target_ns)

    def advance_ns(self, ns: int) -> None:
        self._raise_if_non_positive(ns, "ns")
        self.__advance_to_ns(iTSns(int(self._now_ns) + ns))

    def advance_us(self, us: int) -> None:
        self._raise_if_non_positive(us, "us")
        self.advance_ns(us * 1_000)

    def advance_ms(self, ms: int) -> None:
        self._raise_if_non_positive(ms, "ms")
        self.advance_ns(ms * 1_000_000)

    def advance_s(self, seconds: int | float) -> None:
        self._raise_if_non_positive(seconds, "seconds")
        self.advance_ns(round(seconds * 1_000_000_000))

    def sleep(self, delta: dTS) -> None:
        start_ns = self._now_ns
        target_ns = self.__target_ns_from_delta(start_ns, delta)
        self.sleep_ns(int(target_ns) - int(start_ns))

    def sleep_ns(self, ns: int) -> None:
        self._raise_if_negative(ns, "ns")
        if ns == 0:
            return
        self.__advance_to_ns(iTSns(int(self._now_ns) + ns))

    def sleep_us(self, us: int) -> None:
        self._raise_if_negative(us, "us")
        self.sleep_ns(us * 1_000)

    def sleep_ms(self, ms: int) -> None:
        self._raise_if_negative(ms, "ms")
        self.sleep_ns(ms * 1_000_000)

    def sleep_s(self, seconds: int | float) -> None:
        self._raise_if_negative(seconds, "seconds")
        if seconds == 0:
            return
        self.sleep_ns(round(seconds * 1_000_000_000))

    def wait_and_execute_ns(self, target_ns: int, operation: Callable[[], None]) -> None:
        if target_ns <= self._now_ns:
            operation()
            return
        heapq.heappush(self._operations, (target_ns, self._sequence, operation))
        self._sequence += 1

    def __advance_to_ns(self, target_ns: iTSns) -> None:
        if target_ns <= self._now_ns:
            raise ValueError("target time must be after current time")
        self.__move_to_ns(target_ns)

    def __move_to_ns(self, target_ns: iTSns) -> None:
        while self._operations and self._operations[0][0] <= target_ns:
            operation_ns, _, operation = heapq.heappop(self._operations)
            self._now_ns = iTSns(operation_ns)
            operation()
        self._now_ns = target_ns


class iTSProviderMock(iTSBaseProviderMock, iTSProvider):
    def now(self) -> iTS:
        return iTS(self._now_ns)


class iTSmsProviderMock(iTSBaseProviderMock, iTSmsProvider):
    def now(self) -> iTSms:
        return iTSms(self._now_ns)


class iTSusProviderMock(iTSBaseProviderMock, iTSusProvider):
    def now(self) -> iTSus:
        return iTSus(self._now_ns)


class iTSnsProviderMock(iTSBaseProviderMock, iTSnsProvider):
    def now(self) -> iTSns:
        return self._now_ns
