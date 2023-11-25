#!/usr/bin/env python
# coding:utf-8
# Author: ASU --<andrei.suiu@gmail.com>
# Purpose:
# Created: 8/4/2021

__author__ = "ASU"

import math
import re
import sys
import warnings
from abc import ABC, abstractmethod, ABCMeta
from datetime import datetime, timezone, tzinfo, timedelta
from decimal import Decimal
from numbers import Integral, Real, Number
from time import time_ns
from typing import Union, Optional, Tuple

import ciso8601
import pytz
from _decimal import InvalidOperation
from dateutil import parser as date_util_parser
from dateutil.relativedelta import relativedelta
from typing_extensions import Literal

if sys.version_info >= (3, 11):
    DEFAULT_ISO_PARSER = datetime.fromisoformat
else:
    DEFAULT_ISO_PARSER = ciso8601.parse_datetime
FIRST_MONDAY_TS = 345600
DAY_SEC = 24 * 3600
DAY_MSEC = DAY_SEC * 1000
DAY_NSEC = DAY_SEC * 1_000_000_000
WEEK_SEC = 7 * DAY_SEC


class dTS:
    """
    Represents a delta timestamp.
    It can be used to represent a delta in time, like 1h, 1d, 1w, 1M, 1Y, etc.
    It can be used to add or subtract a delta from a timestamp.
    It can be used to represent a delta in months, like 1M, 2M, 3M, etc.
    It can be used to represent a delta in years, like 1Y, 2Y, 3Y, etc.
    """
    PATTERN_RE = re.compile(r"^(-?\d+)(ms|ns|s|m|h|d|w|M|Y)$")
    NS_BY_UNIT = {
        'w':  7 * 24 * 60 * 60 * 1_000_000_000,
        'd':  24 * 60 * 60 * 1_000_000_000,
        'h':  60 * 60 * 1_000_000_000,
        'm':  60 * 1_000_000_000,
        's':  1_000_000_000,
        'ms': 1_000_000,
        'us': 1_000,
        'ns': 1,
    }

    @classmethod
    def _get_deltas(cls, delta: int, unit: str) -> Tuple[int, int]:
        """
        Returns the delta in nanoseconds & months
        :param delta: the delta in the specified unit
        :param unit: (default: sec) the unit of the delta, if it's not specified, it will be parsed from the delta string

        :return: (delta_ns, months)
        """
        if unit == "M":
            return 0, delta
        if unit == "Y":
            return 0, delta * 12
        delta_ns = round(delta * cls.NS_BY_UNIT[unit])
        return delta_ns, 0

    def __init__(self, delta: Union[str, Number, timedelta], unit: Optional[Literal['Y', 'M', 'w', 'd', 'h', 'm', 's', 'ms', 'us', 'ns']] = None) -> None:
        """
        :param delta: the delta in the specified unit
        :param unit: (default: sec) the unit of the delta, if it's not specified, it will be parsed from the delta string
        """
        if isinstance(delta, str):
            m = self.PATTERN_RE.match(delta)
            if not m:
                raise ValueError(f"Invalid delta string: {delta}")
            delta, unit = m.groups()
            delta = int(delta)
            self._delta_ns, self._months = self._get_deltas(delta, unit)
        elif isinstance(delta, timedelta):
            raise NotImplementedError("timedelta is not supported yet")
        elif isinstance(delta, Number):
            unit = unit or "s"
            delta = round(delta)
            self._delta_ns, self._months = self._get_deltas(delta, unit)

    @staticmethod
    def _add_raw(ts_ns: int, delta_ns: int, months: int) -> int:
        if not type(ts_ns) is int:
            ts_ns = int(ts_ns)
        if months != 0:
            day_ns = ts_ns % DAY_NSEC
            date_ts = (ts_ns - day_ns) // 1_000_000_000
            dt = datetime.fromtimestamp(date_ts, tz=timezone.utc)
            months_delta = relativedelta(months=months)
            new_dt: datetime = dt + months_delta
            new_ts_ns = round(new_dt.timestamp()) * 1_000_000_000 + day_ns + delta_ns
            return new_ts_ns
        return ts_ns + delta_ns

    def _add(self, ts_ns: int) -> int:
        """
        Adds the delta to the timestamp in nanoseconds
        :param ts_ns: the timestamp in nanoseconds
        """
        return self._add_raw(ts_ns, self._delta_ns, self._months)

    def _sub(self, ts_ns: int) -> int:
        """
        Subtracts the delta from the timestamp in nanoseconds
        :param ts_ns: the timestamp in nanoseconds
        """
        return self._add_raw(ts_ns, -self._delta_ns, -self._months)

    def __str__(self) -> str:
        if self._months != 0:
            assert self._delta_ns == 0
            if self._months % 12 == 0:
                return f"{self._months // 12}Y"
            return f"{self._months}M"
        for unit, ns in self.NS_BY_UNIT.items():
            if self._delta_ns % ns == 0:
                return f"{self._delta_ns // ns}{unit}"
        raise RuntimeError(f"dTS class corrupted: dTS(delta_ns={self._delta_ns}, months={self._months})")

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}("{self.__str__()}")'

    def __eq__(self, o: "dTS") -> bool:
        if not isinstance(o, dTS):
            return False
        return self._months == o._months and self._delta_ns == o._delta_ns

    def __radd__(self, other):
        return self.__add__(other)


class BaseTS(ABC, metaclass=ABCMeta):
    @classmethod
    def __get_validators__(cls):
        yield cls._pydantic_validator

    @classmethod
    def _pydantic_validator(cls, v):
        if isinstance(v, cls):
            return v
        if isinstance(v, (str, Integral, Real)):
            try:
                return cls(v)
            except Exception:
                raise TypeError(
                    f"{repr(v)} fo class {type(v)} CAN'T be converted to {cls}"
                )
        raise TypeError(f"{repr(v)} fo class {type(v)} CAN'T be converted to {cls}")

    @classmethod
    def timestamp_from_iso(cls, ts: str, utc: bool = True) -> float:
        """
        Attention: if timestamp has TZ info, it will ignore the utc parameter
        This method exists because dateutil.parser is too generic and wrongly parses basic ISO date like `20210101`
        It will allow any of ISO-8601 formats, but will not allow any other formats
        """
        dt = DEFAULT_ISO_PARSER(ts)
        if utc and dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        float_val = dt.timestamp()
        return float_val

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
                return cls.timestamp_from_iso(ts, utc)
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

    @abstractmethod
    def timestamp(self) -> "TS":
        """
        Return POSIX timestamp corresponding to the datetime instance. The return value is a float similar to that returned by time.time().
        Returns the same as datetime.timestamp() but it is timezone aware
        Note: The returned value in fact is a TS instance, but since it's a subclass of float, it can be used as a float.

        :return: TS
        """
        raise NotImplementedError()

    def as_dt(self, tz: tzinfo = timezone.utc) -> datetime:
        """
        Returns an "aware" datetime object in UTC by default
        """
        ts = self.timestamp()
        return datetime.fromtimestamp(ts, tz=tz)

    def as_local_dt(self) -> datetime:
        """
        Returns an "aware" datetime object in local time
        Note: We need to call astimezone as fromtimestamp returns a naive datetime otherwise
        """
        return datetime.fromtimestamp(self.timestamp(), tz=None).astimezone()

    def isoformat(self, sep='T', timespec='auto') -> str:
        """
        Return a string representing the date and time in ISO 8601 format:
            YYYY-MM-DDTHH:MM:SS.ffffff, if microsecond is not 0
            YYYY-MM-DDTHH:MM:SS, if microsecond is 0

        If utcoffset() does not return None, a string is appended, giving the UTC offset:
            YYYY-MM-DDTHH:MM:SS.ffffff+HH:MM[:SS[.ffffff]], if microsecond is not 0
            YYYY-MM-DDTHH:MM:SS+HH:MM[:SS[.ffffff]], if microsecond is 0

        - The optional argument sep (default 'T') is a one-character separator, placed between the date and time portions of the result.
            For example: 2002-12-25T00:00:00-06:39

        The optional argument timespec specifies the number of additional components of the time to include (the default is 'auto'). It can be one of the following:
            'auto': Same as 'seconds' if microsecond is 0, same as 'microseconds' otherwise.
            'hours': Include the hour in the two-digit HH format.
            'minutes': Include hour and minute in HH:MM format.
            'seconds': Include hour, minute, and second in HH:MM:SS format.
            'milliseconds': Include full time, but truncate fractional second part to milliseconds. HH:MM:SS.sss format.
            'microseconds': Include full time in HH:MM:SS.ffffff format.

        Note: Excluded time components are truncated, not rounded.
        """
        s = self.as_dt().isoformat(sep=sep, timespec=timespec)
        s = s.replace("+00:00", "Z")
        return s

    iso = isoformat

    def iso_date(self) -> str:
        """
        Returns Extended ISO date format.
        Example: 2021-01-01
        """
        s = self.as_dt().strftime("%Y-%m-%d")
        return s

    def iso_date_basic(self) -> str:
        """
        Returns Basic ISO date format.
        Example: 20210101
        """
        s = self.as_dt().strftime("%Y%m%d")
        return s

    def iso_tz(self, tz: Union[str, tzinfo]) -> str:
        """
        Returns ISO date format with TZ info.
        Example: 2021-01-01
        """
        if isinstance(tz, str):
            tz = pytz.timezone(tz)
        dt = self.as_dt(tz=tz)
        s = dt.isoformat()
        s = s.replace("+00:00", "Z")
        return s

    def iso_basic(self) -> str:
        """
        Returns Basic ISO date format.
        Example: 20210101-000000
        """
        dt = self.as_dt()
        s = dt.strftime("%Y%m%d-%H%M%S")
        return s

    def as_sec(self) -> "iTS":
        """
        Converts to iTS (integer timestamp in seconds)
        Note: it will round the timestamp to seconds
        """
        return iTS(round(self.timestamp()))

    def as_msec(self) -> "iTSms":
        """
        Converts to iTSms (integer timestamp in milliseconds).
        Note: it will round the timestamp to milliseconds
        """
        return iTSms(round(self.timestamp() * 1000))

    def as_usec(self) -> "iTSus":
        """
        Converts to iTSus (integer timestamp in microseconds)
        Note: it will round the timestamp to microseconds
        """
        return iTSus(round(self.timestamp() * 1_000_000))

    def as_nsec(self) -> "iTSns":
        """
        Converts to iTSns (integer timestamp in nanoseconds)
        Note: it will round the timestamp to nanoseconds
        """
        return iTSns(round(self.timestamp() * 1_000_000_000))

    @abstractmethod
    def floor(self, unit: Union[int, float]) -> "BaseTS":
        """
        Returns the timestamp floored to the specified unit.

        :param unit: the unit in is expressed in timeunits of the same precision as the class, i.e. for iTSms it's ms, and for TS it's sec
        """
        raise NotImplementedError()

    @abstractmethod
    def ceil(self, unit: int) -> "BaseTS":
        """
        Returns the timestamp ceiled to the specified unit
        :param unit: the unit to ceil which should be of the same precision as the timestamp
        """
        raise NotImplementedError()

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
        return self.isoformat()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.isoformat()!r})"

    def __add__(self, x: float) -> "BaseTS":
        return self.__class__(super().__add__(x))

    def __radd__(self, other):
        return self.__add__(other)

    def __sub__(self, x: float) -> "BaseTS":
        return type(self)(super().__sub__(x))

    def __rsub__(self, x: float) -> "BaseTS":
        return type(self)(super().__rsub__(x))


class TS(BaseTS, float):
    """
    Represents Unix timestamp in seconds since Epoch, by default in UTC.
    It can use local time-zone if utc=False is specified at construction.

    This class is a subclass of float, so it can be used as a float, but it also has some extra methods.
    """

    @staticmethod
    def now_dt() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def now_ms() -> "iTSms":
        return iTSms.now()

    @staticmethod
    def now_us() -> "iTSus":
        return iTSus.now()

    @staticmethod
    def now_ns() -> "iTSns":
        return iTSns.now()

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
        float_val = cls.timestamp_from_iso(ts, utc)
        return cls(float_val, prec="s")

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

    def __init__(self, *args, **kwargs) -> None:
        """ This empty method is required by static analysis tools/IDEs to work properly for auto-completion """
        pass

    def timestamp(self) -> "TS":
        return self

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
    def as_ms(self) -> "iTSms":
        """
        Represents Unix timestamp in MilliSeconds since Epoch

        # ToDo: This property will be removed after few versions, so now it's deprecated
        """
        warnings.warn(f"Call to deprecated property TS.as_ms", category=DeprecationWarning, stacklevel=2)
        return iTSms(self)

    @property
    def as_sec(self) -> "iTS":
        """
        Represents Unix timestamp in MilliSeconds since Epoch

        # ToDo: This will be transformed into the method after few versions, so now this property is deprected
            For now we can use iTS(ts) which is equivalent
        """
        warnings.warn(f"Call to deprecated property TS.as_ms", category=DeprecationWarning, stacklevel=2)
        return iTS(self)

    def to_sec(self) -> "iTS":
        """
        Represents Unix timestamp in seconds since Epoch

        ToDo: this is a temporary replacement of as_sec
        """
        return iTS(self)

    def floor(self, unit: float) -> "TS":
        """
        Returns the timestamp floored to the specified unit.
        unit is rounded to the ms precision, so it can be 0.100 for example, to floow to 100ms

        :param unit: the unit in seconds, to floor to.
        """
        ms_unit = round(unit * 1000)
        if ms_unit < 1:
            raise ValueError(
                f"Invalid unit for flooring. It should be multiple of milliseconds: {unit}"
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
        return iTS(self)

    def __str__(self) -> str:
        return self.isoformat()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.isoformat()!r})"

    def __add__(self, x: Union[float, dTS]) -> "TS":
        if isinstance(x, dTS):
            return TS(x._add(self.as_nsec()), prec="ns")
        return TS(float.__add__(self, x))

    def __radd__(self, other) -> "TS":
        return self.__add__(other)

    def __sub__(self, x: float) -> "TS":
        if isinstance(x, dTS):
            return TS(x._sub(self.as_nsec()), prec='ns')
        return TS(float.__sub__(self, x))

    def __rsub__(self, x: float) -> "TS":
        return TS(float.__rsub__(self, x))


class TSMsec(TS):
    def __new__(cls, ts: Union[int, float, str], prec: Literal["s", "ms"] = "ms"):
        if isinstance(ts, TS):
            return ts
        return super().__new__(TS, ts, prec)


class iBaseTS(BaseTS, int):
    UNITS_IN_SEC: int = None
    PREC_STR: str = None

    @classmethod
    def now(cls) -> "iBaseTS":
        tns = time_ns()
        precision = 1_000_000_000 // cls.UNITS_IN_SEC
        rounded_val = round(tns / precision)
        return cls(rounded_val)

    @classmethod
    def from_iso(cls, ts: str, utc: bool = True) -> "iBaseTS":
        """
        Attention: if timestamp has TZ info, it will ignore the utc parameter
        This method exists because dateutil.parser is too generic and wrongly parses basic ISO date like `20210101`
        It will allow any of ISO-8601 formats, but will not allow any other formats
        """
        float_val = cls.timestamp_from_iso(ts, utc)
        return cls(float_val * cls.UNITS_IN_SEC)

    def timestamp(self) -> "TS":
        return TS(self, prec=self.PREC_STR)

    def floor(self, unit: int) -> "iBaseTS":
        """
        Returns the timestamp floored to the specified unit.

        :param unit: the unit in is expressed in timeunits of the same precision as the class, i.e. for iTSms it's ms, and for TS it's sec
        """
        assert isinstance(unit, Integral) and unit > 0, f"Invalid unit for flooring. It should be multiple of nanoseconds: {unit}"
        floored_int = (self // unit) * unit
        return type(self)(floored_int)

    def ceil(self, unit: int) -> "iBaseTS":
        """
        Returns the timestamp ceiled to the specified unit
        :param unit: the unit to ceil in seconds
        """
        assert isinstance(unit, Integral) and unit > 0, f"Invalid unit for ceiling. It should be multiple of nanoseconds: {unit}"
        ceiled_int = -(-self // unit) * unit  # this performs a ceiling division
        # ceiled_int = math.ceil(self / unit) * unit
        return type(self)(ceiled_int)


class iTS(iBaseTS):
    """
    Represents Unix timestamp in seconds since Epoch, by default in UTC.
    It can use local time-zone if utc=False is specified at construction.
    This class is a subclass of int, so it can be used as an int, but it also has some extra methods.
    """
    UNITS_IN_SEC: int = 1
    PREC_STR: str = "s"

    def __new__(cls, ts: Union[int, float, str], utc: bool = True):
        if isinstance(ts, iTS):
            return ts
        if isinstance(ts, iTSms):
            return int.__new__(cls, round(ts / 1_000))
        if isinstance(ts, iTSus):
            return int.__new__(cls, round(ts / 1_000_000))
        if isinstance(ts, iTSns):
            return int.__new__(cls, round(ts / 1_000_000_000))
        if isinstance(ts, TS):
            int_val = round(ts.timestamp())
            return int.__new__(cls, int_val)
        float_val = cls._parse_to_float(ts, prec="s", utc=utc)
        int_val = round(float_val)
        return int.__new__(cls, int_val)

    def as_sec(self) -> "iTS":
        return self


class iTSms(iBaseTS):
    """
    Represents Unix timestamp in milliseconds since Epoch, by default in UTC.
    It can use local time-zone if utc=False is specified at construction.
    This class is a subclass of int, so it can be used as an int, but it also has some extra methods.
    """

    UNITS_IN_SEC: int = 1_000
    PREC_STR: str = "ms"

    def __new__(cls, ts: Union[int, float, str], utc: bool = True):
        if isinstance(ts, iTS):
            return iTSus(ts * 1_000)
        if isinstance(ts, iTSms):
            return ts
        if isinstance(ts, iTSus):
            return int.__new__(cls, round(ts / 1_000))
        if isinstance(ts, iTSns):
            return int.__new__(cls, round(ts / 1_000_000))
        if isinstance(ts, TS):
            int_val = round(ts.timestamp() * cls.UNITS_IN_SEC)
            return int.__new__(cls, int_val)
        float_val = cls._parse_to_float(ts, prec="ms", utc=utc)
        int_val = round(float_val * cls.UNITS_IN_SEC)
        return int.__new__(cls, int_val)

    def as_msec(self) -> "iTSms":
        return self


class iTSus(iBaseTS):
    """
    Represents Unix timestamp in microseconds since Epoch, by default in UTC.
    It can use local time-zone if utc=False is specified at construction.
    This class is a subclass of int, so it can be used as an int, but it also has some extra methods.
    """
    UNITS_IN_SEC = 1_000_000
    PREC_STR = "us"

    def __new__(cls, ts: Union[int, float, str], utc: bool = True):
        if isinstance(ts, iTS):
            return iTSus(ts * 1_000_000)
        if isinstance(ts, iTSms):
            return int.__new__(cls, ts * 1_000)
        if isinstance(ts, iTSus):
            return ts
        if isinstance(ts, iTSns):
            return int.__new__(cls, round(ts / 1_000))
        if isinstance(ts, TS):
            int_val = round(ts.timestamp() * cls.UNITS_IN_SEC)
            return int.__new__(cls, int_val)
        if isinstance(ts, Integral):
            return int.__new__(cls, ts)
        if isinstance(ts, Real):
            return int.__new__(cls, round(ts))
        float_val = cls._parse_to_float(ts, prec="us", utc=utc)
        int_val = round(float_val * cls.UNITS_IN_SEC)
        return int.__new__(cls, int_val)

    def as_usec(self) -> "iTSus":
        return self


class iTSns(iBaseTS):
    """
    Represents Unix timestamp in nanoseconds since Epoch, by default in UTC.
    It can use local time-zone if utc=False is specified at construction.
    This class is a subclass of int, so it can be used as an int, but it also has some extra methods.
    """

    UNITS_IN_SEC = 1_000_000_000
    PREC_STR = "ns"

    def __new__(cls, ts: Union[int, float, str], utc: bool = True):
        if isinstance(ts, iTS):
            return iTSus(ts * 1_000_000_000)
        if isinstance(ts, iTSms):
            return int.__new__(cls, ts * 1_000_000)
        if isinstance(ts, iTSus):
            return int.__new__(cls, ts * 1_000)
        if isinstance(ts, iTSns):
            return ts
        if isinstance(ts, TS):
            int_val = round(ts.timestamp() * cls.UNITS_IN_SEC)
            return int.__new__(cls, int_val)
        if isinstance(ts, Integral):
            return int.__new__(cls, ts)
        if isinstance(ts, Real):
            return int.__new__(cls, round(ts))
        try:
            int_val = int(ts)
            return int.__new__(cls, int_val)
        except ValueError:
            pass
        try:
            dec = Decimal(ts)
            rounded_int = dec.to_integral_value()
            return int.__new__(cls, rounded_int)
        except (ValueError, InvalidOperation):
            pass
        assert isinstance(ts, str), f"Invalid type for ts: {type(ts)}"
        float_val = cls._parse_to_float(ts, prec="ns", utc=utc)
        int_val = round(float_val * cls.UNITS_IN_SEC)
        return int.__new__(cls, int_val)

    @classmethod
    def now(cls) -> "iBaseTS":
        tns = time_ns()
        return cls(tns)

    def as_nsec(self) -> "iTSns":
        return self
