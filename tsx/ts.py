#!/usr/bin/env python
# coding:utf-8
# Author: ASU --<andrei.suiu@gmail.com>
# Purpose: 
# Created: 8/4/2021

__author__ = 'ASU'

import traceback
from datetime import datetime, timezone, tzinfo
from numbers import Integral, Real
from typing import Union

import pytz
from dateutil.parser import parse as parse_date
from pyxtension import validate
from typing_extensions import Literal


class TS(float):
    """
    Represents Unix timestamp in seconds since Epoch
    """

    INFINITE_TIME_TS = "2100-01-01T00:00:00+00:00"

    @staticmethod
    def now_dt() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def now_ms() -> int:
        return int(TS.now_dt().timestamp() * 1000)

    @classmethod
    def now(cls):
        return cls(cls.now_ms(), prec="ms")

    @staticmethod
    def _is_float(s) -> bool:
        try:
            float(s)
            return True
        except Exception:
            return False

    @classmethod
    def _parse_to_float(cls, ts: Union[int, float, str], prec: Literal["s", "ms"]) -> float:
        if isinstance(ts, float):
            validate(prec == "s")
            return ts
        elif isinstance(ts, int):
            if prec == "s":
                return float(ts)
            else:
                return float(ts / 1000)
        elif prec == "s" and cls._is_float(ts):
            return float(ts)
        elif prec == "ms" and cls._is_float(ts):
            return float(ts) / 1000.0
        elif isinstance(ts, str):
            try:
                dt = parse_date(ts)
                float_val = dt.timestamp()
                return float_val
            except Exception:
                raise ValueError(f"The value can't be converted to TimeStamp: {ts!s} Stack:\n{traceback.format_exc()}")
        raise ValueError(f"The value can't be converted to TimeStamp: {ts!s}")

    def __new__(cls, ts: Union[int, float, str], prec: Literal["s", "ms"] = "s"):
        float_val = cls._parse_to_float(ts, prec)
        return float.__new__(cls, float_val)

    @property
    def as_iso(self) -> str:
        dt = datetime.fromtimestamp(self, tz=timezone.utc)
        s = dt.isoformat()
        s = s.replace("+00:00", "Z")
        return s

    @property
    def as_iso_date(self) -> str:
        dt = datetime.fromtimestamp(self, tz=timezone.utc)
        s = dt.strftime("%Y-%m-%d")
        return s

    def as_iso_tz(self, tz: Union[str, tzinfo]) -> str:
        if isinstance(tz, str):
            tz = pytz.timezone(tz)
        dt = datetime.fromtimestamp(self, tz=tz)
        s = dt.isoformat()
        s = s.replace("+00:00", "Z")
        return s

    @property
    def as_file_ts(self) -> str:
        dt = datetime.fromtimestamp(self, tz=timezone.utc)
        s = dt.strftime("%Y%m%d-%H%M%S")
        return s

    @property
    def as_file_date(self) -> str:
        dt = datetime.fromtimestamp(self, tz=timezone.utc)
        s = dt.strftime("%Y%m%d")
        return s

    @property
    def as_ms(self) -> int:
        """
        Represents Unix timestamp in MilliSeconds since Epoch
        """
        return int(round(self * 1000))

    @property
    def as_sec(self) -> int:
        """
        Represents Unix timestamp in MilliSeconds since Epoch
        """
        return self.__int__()

    @classmethod
    def __get_validators__(cls):
        yield cls._pydantic_validator

    @classmethod
    def _pydantic_validator(cls, v):
        if isinstance(v, (str, Integral, Real)):
            try:
                return cls(v, prec="s")
            except Exception:
                raise TypeError(f"{repr(v)} fo class {type(v)} CAN'T be converted to {cls}")
        elif isinstance(v, TS):
            return v
        raise TypeError(f"{repr(v)} fo class {type(v)} CAN'T be converted to {cls}")

    def __int__(self) -> int:
        return round(self)

    def __str__(self) -> str:
        # ToDo: make it return self.as_iso, but check all the places where this can be str-ed, like
        # SourceUtils.get_tick_df

        i = int(self)
        if i == self:
            return str(i)
        return str(float(self))

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.as_iso!r})"

    def __add__(self, x: float) -> "TS":
        return TS(super().__add__(x))

    def __sub__(self, x: float) -> "TS":
        return TS(super().__sub__(x))


class TSMsec(TS):
    def __new__(cls, ts: Union[int, float, str], prec: Literal["s", "ms"] = "ms"):
        return super().__new__(cls, ts, prec)

    @classmethod
    def __get_validators__(cls):
        yield cls._pydantic_validator

    @classmethod
    def _pydantic_validator(cls, v):
        if isinstance(v, (str, Integral, Real)):
            try:
                return cls(v, prec="ms")
            except Exception:
                raise TypeError(f"{repr(v)} fo class {type(v)} CAN'T be converted to {cls}")
        elif isinstance(v, TS):
            return v
        raise TypeError(f"{repr(v)} fo class {type(v)} CAN'T be converted to {cls}")
