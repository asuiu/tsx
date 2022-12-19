#!/usr/bin/env python
# coding:utf-8
# Author: ASU --<andrei.suiu@gmail.com>
# Purpose:
# Created: 8/4/2021

__author__ = "ASU"

import math
import traceback
from datetime import datetime, timezone, tzinfo
from numbers import Integral, Real
from typing import Union

import pytz
from dateutil import parser as date_util_parser
from pyxtension import validate
from typing_extensions import Literal

DEFAULT_ISO_PARSER = date_util_parser.isoparser()
_FIRST_MONDAY_TS = 345600


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
    def now(cls) -> "TS":
        return cls(cls.now_ms(), prec="ms")

    @classmethod
    def from_iso(cls, ts: str, utc: bool = True) -> "TS":
        """
        Attention: if timestamp has TZ info, it will ignore the utc parameter
        This method exists because dateutil.parser is too generic and wrongly parses basic ISO date like `20210101`
        It will allow any of ISO-8601 formats, but will not allow any other formats
        """
        dt = date_util_parser.isoparser().isoparse(ts)
        if utc and dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        float_val = dt.timestamp()
        return cls(float_val)

    @staticmethod
    def _is_float(s) -> bool:
        try:
            float(s)
            return True
        except Exception:
            return False

    @classmethod
    def _parse_to_float(
            cls, ts: Union[int, float, str], prec: Literal["s", "ms"]
    ) -> float:
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
                dt = date_util_parser.parse(ts)
                float_val = dt.timestamp()
                return float_val
            except Exception:
                raise ValueError(
                    f"The value can't be converted to TimeStamp: {ts!s} Stack:\n{traceback.format_exc()}"
                )
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
        """Returns Extended ISO date format"""
        dt = datetime.fromtimestamp(self, tz=timezone.utc)
        s = dt.strftime("%Y-%m-%d")
        return s

    @property
    def as_iso_date_basic(self) -> str:
        """Returns Basic ISO date format"""
        dt = datetime.fromtimestamp(self, tz=timezone.utc)
        s = dt.strftime("%Y%m%d")
        return s

    def as_iso_tz(self, tz: Union[str, tzinfo]) -> str:
        if isinstance(tz, str):
            tz = pytz.timezone(tz)
        dt = datetime.fromtimestamp(self, tz=tz)
        s = dt.isoformat()
        s = s.replace("+00:00", "Z")
        return s

    @property
    def as_iso_basic(self) -> str:
        dt = datetime.fromtimestamp(self, tz=timezone.utc)
        s = dt.strftime("%Y%m%d-%H%M%S")
        return s

    as_file_ts = as_iso_basic
    as_file_date = as_iso_date_basic

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
                raise TypeError(
                    f"{repr(v)} fo class {type(v)} CAN'T be converted to {cls}"
                )
        elif isinstance(v, TS):
            return v
        raise TypeError(f"{repr(v)} fo class {type(v)} CAN'T be converted to {cls}")

    def floor(self, unit: float) -> "TS":
        """
        Returns the timestamp floored to the specified unit
        :param unit: the unit to floor to which should be multiple of milliseconds
        """
        ms_unit = round(unit * 1000)
        if ms_unit < 1:
            raise ValueError(
                f"Invalid unit for ceiling. It should be multiple of milliseconds: {unit}"
            )
        self_ms = self * 1000
        return TS(math.floor(self_ms / ms_unit) * ms_unit / 1000)

    def ceil(self, unit: float) -> "TS":
        """
        Returns the timestamp ceiled to the specified unit
        :param unit: the unit to ceil to which should be multiple of milliseconds
        """
        ms_unit = round(unit * 1000)
        if ms_unit < 1:
            raise ValueError(
                f"Invalid unit for ceiling. It should be multiple of milliseconds: {unit}"
            )
        self_ms = self * 1000
        return TS(math.ceil(self_ms / ms_unit) * ms_unit / 1000)

    def weekday(self, utc: bool = True) -> int:
        """
        Return the day of the week as an integer, where Monday is 0 and Sunday is 6. See also isoweekday().
        """
        if utc:
            return int((self - _FIRST_MONDAY_TS) / (24 * 3600)) % 7
        else:
            dt = datetime.fromtimestamp(self)
            return dt.weekday()

    def isoweekday(self, utc: bool = True) -> int:
        """
        Return the day of the week as an integer, where Monday is 1 and Sunday is 7. See also weekday().
        """
        return self.weekday(utc) + 1

    def __int__(self) -> int:
        return round(self)

    def __str__(self) -> str:
        return self.as_iso

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
                raise TypeError(
                    f"{repr(v)} fo class {type(v)} CAN'T be converted to {cls}"
                )
        elif isinstance(v, TS):
            return v
        raise TypeError(f"{repr(v)} fo class {type(v)} CAN'T be converted to {cls}")
