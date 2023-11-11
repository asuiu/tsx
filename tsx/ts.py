#!/usr/bin/env python
# coding:utf-8
# Author: ASU --<andrei.suiu@gmail.com>
# Purpose:
# Created: 8/4/2021

__author__ = "ASU"

import math
import sys
from datetime import datetime, timezone, tzinfo
from numbers import Integral, Real
from typing import Union

import ciso8601
import pytz
from dateutil import parser as date_util_parser
from typing_extensions import Literal

if sys.version_info >= (3, 11):
    DEFAULT_ISO_PARSER = datetime.fromisoformat
else:
    DEFAULT_ISO_PARSER = ciso8601.parse_datetime
FIRST_MONDAY_TS = 345600
DAY_SEC = 24 * 3600
DAY_MSEC = DAY_SEC * 1000
WEEK_SEC = 7 * DAY_SEC


class TS(float):
    """
    Represents Unix timestamp in seconds since Epoch, by default in UTC.
    It can use local time-zon if utc=False is specified at construction.
    """

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
        dt = DEFAULT_ISO_PARSER(ts)
        if utc and dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        float_val = dt.timestamp()
        return cls(float_val, prec="s")

    @staticmethod
    def _is_float(s) -> bool:
        try:
            float(s)
            return True
        except (ValueError, TypeError):
            return False

    @classmethod
    def _from_number(cls, ts: Union[float, int], prec: Literal["s", "ms", "us", "ns"]):
        if prec == "s":
            return ts
        elif prec == "ms":
            return ts / 1000
        elif prec == "us":
            return ts / 1_000_000
        elif prec == "ns":
            return ts / 1_000_000_000
        raise ValueError(f"Invalid precision: {prec}")

    @classmethod
    def _parse_to_float(
        cls,
        ts: Union[int, float, str],
        prec: Literal["s", "ms", "us", "ns"],
        utc: bool = True,
    ) -> float:
        if isinstance(ts, str):
            try:
                return cls.from_iso(ts, utc)
            except Exception:
                pass
            try:
                dt = date_util_parser.parse(ts)
                if utc:
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                float_val = dt.timestamp()
                return float_val
            except Exception:
                pass
        return cls._from_number(float(ts), prec)

    def __new__(
            cls,
            ts: Union[int, float, str],
            prec: Literal["s", "ms", "us", "ns"] = "s",
            utc: bool = True,
    ):
        if isinstance(ts, TS):
            return ts
        float_val = cls._parse_to_float(ts, prec, utc)
        return float.__new__(cls, float_val)

    def as_dt(self, tz: tzinfo = timezone.utc) -> datetime:
        """
        Returns an "aware" datetime object in UTC by default
        """
        return datetime.fromtimestamp(self, tz=tz)

    def as_local_dt(self) -> datetime:
        """
        Returns an "aware" datetime object in local time
        Note: We need to call astimezone as fromtimestamp returns a naive datetime otherwise
        """
        return datetime.fromtimestamp(self, tz=None).astimezone()

    @property
    def as_iso(self) -> str:
        s = self.as_dt().isoformat()
        s = s.replace("+00:00", "Z")
        return s

    @property
    def as_iso_date(self) -> str:
        """Returns Extended ISO date format"""
        s = self.as_dt().strftime("%Y-%m-%d")
        return s

    @property
    def as_iso_date_basic(self) -> str:
        """Returns Basic ISO date format"""
        s = self.as_dt().strftime("%Y%m%d")
        return s

    def as_iso_tz(self, tz: Union[str, tzinfo]) -> str:
        if isinstance(tz, str):
            tz = pytz.timezone(tz)
        dt = self.as_dt(tz=tz)
        s = dt.isoformat()
        s = s.replace("+00:00", "Z")
        return s

    @property
    def as_iso_basic(self) -> str:
        dt = self.as_dt()
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
            return int((self - FIRST_MONDAY_TS) / (24 * 3600)) % 7
        else:
            dt = self.as_local_dt()
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