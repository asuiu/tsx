#!/usr/bin/env python
# coding:utf-8
# Author: ASU --<andrei.suiu@gmail.com>
# Purpose:
# Created: 8/4/2021

__author__ = "ASU"

import pickle
import sys
import unittest
from _decimal import Decimal
from datetime import datetime, timezone, date, timedelta
from time import time, localtime, strftime, time_ns
from typing import Type
from unittest import TestCase
from unittest.mock import patch

import numpy as np
import pytz
from dateutil import tz
from pydantic import BaseModel

from tsx import TS, TSMsec, iTS, iTSms, iTSus, iTSns, TSInterval
from tsx.ts import dTS, BaseTS


def run_test_from_iso(self: TestCase, cls: Type[BaseTS]):
    self.assertIsInstance(cls.from_iso("2018"), cls)
    self.assertEqual(cls.from_iso("2018"), cls("2018-01-01T00:00:00Z"))
    self.assertEqual(cls.from_iso("2018Z"), cls("2018-01-01T00:00:00Z"))
    self.assertEqual(cls.from_iso("201801"), cls("2018-01-01T00:00:00Z"))
    self.assertEqual(cls.from_iso("201801Z"), cls("2018-01-01T00:00:00Z"))
    self.assertEqual(cls.from_iso("2018-01"), cls("2018-01-01T00:00:00Z"))
    self.assertEqual(cls.from_iso("2018-01Z"), cls("2018-01-01T00:00:00Z"))

    ts = cls.from_iso("2018-02-28")
    self.assertEqual(ts, cls("2018-02-28T00:00:00Z"))
    ts = cls.from_iso("2018-02-28", utc=False)
    self.assertEqual(ts, cls("2018-02-28T00:00:00", utc=False))

    ts = cls.from_iso("2018-02-28Z")
    self.assertEqual(ts, cls("2018-02-28T00:00:00Z", utc=False))

    ts = cls.from_iso("20180228")
    self.assertEqual(ts, cls("2018-02-28T00:00:00Z"))

    ts = cls.from_iso("20180228Z")
    self.assertEqual(ts, cls("2018-02-28T00:00:00Z"))

    ts = cls.from_iso("2018-02-28T22:00:00+00:00")
    self.assertEqual(ts, cls("2018-02-28T22:00:00Z"))

    ts = cls.from_iso("2018-02-28T00:00:00+02:00")
    self.assertEqual(ts, cls("2018-02-27T22:00:00Z"))

    if sys.version_info <= (3, 11):
        ts = cls.from_iso("2018-02")
        self.assertEqual(ts, cls("2018-02-01T00:00:00Z"))


class TestTS(TestCase):
    INT_BASE_TS = 1519855200
    INT_BASE_ROUND_MS_TS = 1519855200000
    INT_BASE_MS_TS = 1519855200123
    FLOAT_MS_TS = 1519855200.123856
    STR_SEC_TS = "2018-02-28T22:00:00Z"
    AS_FILE_SEC_TS = "20180228-220000"
    STR_MSEC_TS = "2018-02-28T22:00:00.123Z"

    def _get_tz_delta(self, dt: datetime) -> float:
        return tz.tzlocal().utcoffset(dt).total_seconds()

    def test_from_ms(self):
        ts = TS(ts=self.INT_BASE_ROUND_MS_TS + 500, prec="ms")
        self.assertEqual(ts, self.INT_BASE_TS + 0.5)
        # round up
        ts = TS(ts=self.INT_BASE_ROUND_MS_TS + 501, prec="ms")
        self.assertEqual(ts, self.INT_BASE_TS + 0.501)

    def test_from_ms_str(self):
        ts = TS(ts=str(self.INT_BASE_ROUND_MS_TS + 500), prec="ms")
        self.assertEqual(ts, self.INT_BASE_TS + 0.5)
        # round up
        ts = TS(ts=self.INT_BASE_ROUND_MS_TS + 501, prec="ms")
        self.assertEqual(ts, self.INT_BASE_TS + 0.501)

    def test_now(self):
        ts_ns = 1_000_000_123_456_789
        with patch('tsx.ts.time_ns', return_value=ts_ns):
            ts = TS.now()
        rounded_ts_ns = round(ts * 1e9)
        self.assertEqual(rounded_ts_ns, ts_ns)

    def test_TSMsec_nominal(self):
        ts = TSMsec(ts=str(self.INT_BASE_ROUND_MS_TS + 500))
        self.assertEqual(ts, self.INT_BASE_TS + 0.5)
        # round up
        ts = TS(ts=self.INT_BASE_ROUND_MS_TS + 501, prec="ms")
        self.assertEqual(ts, self.INT_BASE_TS + 0.501)

    def test_from_float_str(self):
        ts = TS(ts="1519855200.123856", prec="s")
        self.assertEqual(ts, self.FLOAT_MS_TS)

    def test_from_int_str(self):
        ts = TS(ts="1519855200")
        self.assertEqual(ts, 1519855200)

    def test_from_datetime(self):
        dt = datetime(2022, 12, 7, 1, 2, 3, tzinfo=timezone.utc)
        ts = TS(ts=dt)
        self.assertEqual(ts, TS("2022-12-07T01:02:03Z"))

    def test_from_datetime_local(self):
        dt = datetime(2022, 12, 7, 1, 2, 3, tzinfo=tz.tzlocal())
        ts = TS(ts=dt)
        self.assertEqual(ts, TS("2022-12-07T01:02:03", utc=False))

    def test_from_date(self):
        dt = date(2022, 12, 7)
        ts = TS(ts=dt)
        self.assertEqual(ts, TS("2022-12-07T00:00:00Z"))

    def test_from_date_local(self):
        dt = date(2022, 12, 7)
        ts = TS(ts=dt, utc=False)
        self.assertEqual(ts, TS("2022-12-07T00:00:00", utc=False))

    def test_from_big_datetime(self):
        dt = datetime(9999, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
        ts = TS(ts=dt)
        self.assertEqual(ts, TS("9999-12-31T23:59:59Z"))

    def test_from_big_date(self):
        dt = date(9999, 12, 31)
        ts = TS(ts=dt)
        self.assertEqual(ts, TS("9999-12-31T00:00:00Z"))

    def test_to_int(self):
        ts = TS(ts=self.INT_BASE_ROUND_MS_TS + 500, prec="ms")
        its = int(ts)
        self.assertIs(type(its), int)
        self.assertEqual(its, self.INT_BASE_TS)
        # round up
        ts = TS(ts=self.INT_BASE_ROUND_MS_TS + 501, prec="ms")
        its = int(ts)
        self.assertIs(type(its), int)
        self.assertEqual(its, self.INT_BASE_TS + 1)

    def test_as_iso(self):
        ts = TS(ts=self.INT_BASE_TS)
        self.assertEqual(ts.as_iso, self.STR_SEC_TS)
        ts = TS(ts=self.INT_BASE_TS) + 0.123456
        self.assertEqual(ts.as_iso, "2018-02-28T22:00:00.123456Z")

    def test_from_reduced_iso_str_through_date_util_parser_explicit_TZ_no_utc(self):
        ts = TS("20221202235700Z", utc=False)
        expected_dt_utc = datetime.fromisoformat("2022-12-02T23:57:00").replace(tzinfo=timezone.utc)
        expected_ts_utc = expected_dt_utc.timestamp()
        self.assertEqual(ts, expected_ts_utc)

    def test_from_reduced_iso_str_through_date_util_parser_no_TZ_no_utc(self):
        ts = TS("20221202235700", utc=False)
        expected_dt = datetime.fromisoformat("2022-12-02T23:57:00")
        expected_ts = expected_dt.timestamp()
        self.assertEqual(ts, expected_ts)

    def test_from_reduced_iso_str_through_date_util_parser_explicit_TZ_with_utc_flag(self):
        ts = TS("20221202235700Z")
        expected_ts = TS("2022-12-02T23:57:00Z")
        self.assertEqual(ts, expected_ts)

    def test_from_reduced_iso_str_through_date_util_parser_no_TZ_with_utc_flag(self):
        ts = TS("20221202235700")
        expected_ts = TS("2022-12-02T23:57:00Z")
        self.assertEqual(ts, expected_ts)

    def test_as_iso_tz_standard(self):
        ts = TS("2018-03-01T00:00:00Z")
        res = ts.as_iso_tz(pytz.timezone("Europe/Bucharest"))
        self.assertEqual(res, "2018-03-01T02:00:00+02:00")
        res = ts.as_iso_tz("Europe/Bucharest")
        self.assertEqual(res, "2018-03-01T02:00:00+02:00")

    def test_as_iso_tz_DST(self):
        ts = TS("2020-06-01T10:00:00Z")
        res = ts.as_iso_tz(pytz.timezone("Europe/Bucharest"))
        self.assertEqual(res, "2020-06-01T13:00:00+03:00")

    def test_as_file_ts(self):
        ts = TS(ts=self.INT_BASE_TS)
        self.assertEqual(ts.as_file_ts, self.AS_FILE_SEC_TS)

    def test_to_str(self):
        ts = TS(ts=self.INT_BASE_TS)
        self.assertEqual(str(ts), '2018-02-28T22:00:00Z')
        float_ts = TS(ts=self.FLOAT_MS_TS)
        self.assertEqual(str(float_ts), '2018-02-28T22:00:00.123856Z')

    def test_input_float_rounds(self):
        # round down
        ts = TS(ts=1519855200.123456)
        ms = ts.as_msec()
        self.assertIsInstance(ms, iTSms)
        self.assertEqual(ms, 1519855200123)
        # round up
        ts = TS(ts=1519855200.123856)
        ms = ts.as_msec()
        self.assertIsInstance(ms, iTSms)
        self.assertEqual(ms, 1519855200124)

    def test_as_str_ms(self):
        ts = TS(ts=self.INT_BASE_MS_TS, prec="ms")
        self.assertEqual("2018-02-28T22:00:00.123000Z", ts.as_iso)

    def test_convert_from_str(self):
        ts = TS(ts="2018-02-28T22:00:00Z")
        self.assertEqual(ts, self.INT_BASE_TS)
        ts = TS(ts="2018-02-28T22:00:00+00:00")
        self.assertEqual(ts, self.INT_BASE_TS)

    def test_convert_from_str_with_TZ_2(self):
        ts = TS(ts="2018-02-28T22:00:00+01:30")
        self.assertEqual(ts, self.INT_BASE_TS - 1.5 * 3600)

    def test_convert_from_ms_str_local_time(self):
        tz_delta_in_sec = self._get_tz_delta(datetime(2018, 2, 28))
        ts = TS(ts="2018-02-28T22:00:00.123", utc=False) + tz_delta_in_sec
        utc_ms_ts = TS("2018-02-28T22:00:00.123Z")
        self.assertEqual(utc_ms_ts, ts)

    def test_convert_from_ms_str_with_TZ_0(self):
        ts = TS(ts="2018-02-28T22:00:00.123+00:00")
        self.assertEqual(ts, self.INT_BASE_MS_TS / 1000)

        ts = TS(ts="2018-02-28T22:00:00.123Z")
        self.assertEqual(ts, self.INT_BASE_MS_TS / 1000)

    def test_convert_from_infinite_dates(self):
        ts = TS('9999-12-31T23:59:59.999999Z')
        self.assertEqual(ts, 253402300799.999999)

        ts = TS('9999-12-31')
        self.assertEqual(ts, 253402214400.0)

    def test_as_dt_infinite_dates(self):
        ts = TS('9999-12-31')
        self.assertEqual(ts.as_dt(), datetime(9999, 12, 31, tzinfo=timezone.utc))

        ts = TS('9999-12-31T23:59:59')
        self.assertEqual(ts.as_dt(), datetime(9999, 12, 31, 23, 59, 59, tzinfo=timezone.utc))

    def test_str_infinite_dates(self):
        ts = TS('9999-12-31')
        self.assertEqual(str(ts), '9999-12-31T00:00:00Z')

        ts = TS('9999-12-31T23:59:59')
        self.assertEqual(str(ts), '9999-12-31T23:59:59Z')

    def test_math_ops(self):
        ts = TS(ts=1519855200)
        self.assertIsInstance(ts + 20, TS)
        self.assertIsInstance(20 + ts, TS)
        self.assertIs(type(ts * 1), float)
        self.assertIs(type(1 * ts), float)
        self.assertIs(type(ts / 1), float)
        self.assertIs(type(100000 / ts), float)

        self.assertEqual((ts + 20).as_msec(), 1519855220000)
        self.assertEqual(TS(20 + ts).as_msec(), 1519855220000)
        self.assertEqual((ts - 0.5).as_msec(), 1519855199500)

    def test_as_iso_date(self):
        ts = TS(ts=self.INT_BASE_TS)
        self.assertEqual(ts.as_iso_date, "2018-02-28")

    def test_as_iso_date_basic(self):
        ts = TS(ts=self.INT_BASE_TS)
        self.assertEqual(ts.as_iso_date_basic, "20180228")
        self.assertEqual(ts.as_file_date, "20180228")

    def test_repr(self):
        ts = TS(ts=self.INT_BASE_TS)
        self.assertEqual(repr(ts), "TS('2018-02-28T22:00:00Z')")

    def test_from_iso(self):
        run_test_from_iso(self, TS)

    def test_floor_over_ms(self):
        ts = TS.from_iso("2018-02-28")
        ts_to_floor = ts + 1.99
        expected = ts + 1.9
        floored = ts_to_floor.floor(unit=0.100000000000001)
        self.assertEqual(expected, floored)

        floored = ts_to_floor.floor(unit=0.099999999999999999999)
        self.assertEqual(expected, floored)

        ts_to_floor = ts + 2.999999999999999999999999999999999999
        expected = ts + 2
        floored = ts_to_floor.floor(unit=2.0)
        self.assertEqual(expected, floored)

    def test_floor_1ms(self):
        ts = TS.from_iso("2018-02-28")
        ts_to_floor = ts + 1.1119
        expected = ts + 1.111

        floored = ts_to_floor.floor(unit=0.001)
        self.assertEqual(expected, floored)

        floored = ts_to_floor.floor(unit=0.001000001)
        self.assertEqual(expected, floored)

        floored = ts_to_floor.floor(unit=0.000999)
        self.assertEqual(expected, floored)

    def test_ceil_over_ms(self):
        ts = TS.from_iso("2018-02-28")
        ts_to_ceil = ts + 1.000001
        expected = ts + 1.1
        # ceil to 100ms
        ceiled = ts_to_ceil.ceil(unit=0.100000000000001)
        self.assertEqual(expected, ceiled)

        ceiled = ts_to_ceil.ceil(unit=0.099999999999999999999)
        self.assertEqual(expected, ceiled)

        ts_to_ceil = ts + 2.000001
        expected = ts + 4
        ceiled = ts_to_ceil.ceil(unit=2.0)
        self.assertEqual(expected, ceiled)

        ts_to_ceil = ts + 2.0000001
        expected = ts + 2
        ceiled = ts_to_ceil.ceil(unit=2.0)
        self.assertEqual(expected, ceiled)

    def test_weekday(self):
        ts = TS.from_iso("2022-12-07T00:00:01", utc=True)
        self.assertEqual(ts.weekday(), 2)

    def test_isoweekday(self):
        ts = TS.from_iso("2022-12-07T00:00:01", utc=True)
        self.assertEqual(ts.isoweekday(), 3)

    def test_default_utc(self):
        expected = TS.from_iso("2022-12-07T00:00:00Z")
        ts = TS("2022-12-07T00:00:00")
        self.assertEqual(expected, ts)
        ts = TS("20221207")
        self.assertEqual(expected, ts)
        t = time()
        print(t)

    def test_local(self):
        unix_ts = int(time())
        lt = localtime(unix_ts)
        iso_str = strftime("%Y-%m-%dT%H:%M:%S", lt)
        local_ts = TS(iso_str, utc=False)
        self.assertEqual(TS(unix_ts), local_ts)

    def test_from_numpy_numbers(self):
        n = np.int64(1519855200000)
        ts = TS(ts=n, prec="ms")
        self.assertEqual(ts, 1519855200)
        ts = TS(ts=np.float64(1519855200.123))
        self.assertEqual(ts, 1519855200.123)

    def test_local_dt_has_tzinfo(self):
        ts = TS("2022-12-07T00:00:00")
        local_tzinfo = datetime(2022, 12, 7).astimezone().tzinfo
        self.assertTrue(ts.as_local_dt().tzinfo is not None)
        self.assertEqual(ts.as_local_dt().tzinfo, local_tzinfo)

    def test_dt_has_tzinfo_in_utc(self):
        ts = TS("2022-12-07T00:00:00Z")
        self.assertEqual(ts.as_dt().tzinfo, timezone.utc)

    def test_TSMsec_from_iso(self):
        ts_ms = TSMsec("2022-12-07T00:00:00.123456Z")
        self.assertEqual(float(ts_ms), 1670371200.123456)

    def test_TSMsec_from_TSMsec(self):
        ts_ms = TSMsec("2022-12-07T00:00:00.123456Z")
        ts_ms2 = TSMsec(ts_ms)
        self.assertEqual(float(ts_ms), float(ts_ms2))

    def test_TS_from_TS(self):
        ts = TS("2022-12-07T00:00:00.123456Z")
        ts2 = TS(ts)
        self.assertEqual(float(ts), float(ts2))

    def test_pydantic_validator(self):
        class TestModel(BaseModel):
            ts: TS
            tsms: TSMsec

        t1 = TestModel(ts="2022-12-07T00:00:00.123456Z", tsms="2022-12-07T00:00:00.123456Z")
        self.assertEqual(float(t1.ts), 1670371200.123456)
        self.assertEqual(float(t1.tsms), 1670371200.123456)

        t1 = TestModel(ts=1670371200.123456, tsms=1670371200123.456)
        self.assertEqual(float(t1.ts), 1670371200.123456)
        self.assertEqual(float(t1.tsms), 1670371200.123456)

        t1 = TestModel(ts=1670371200, tsms=1670371200123)
        self.assertEqual(float(t1.ts), 1670371200)
        self.assertEqual(float(t1.tsms), 1670371200.123)

    def test_as_sec(self):
        ts = TS("2022-12-07T00:00:00.123456Z")
        # ToDo: fix after we make as_sec - method
        sec = ts.as_sec
        self.assertIsInstance(sec, iTS)
        self.assertEqual(sec, 1670371200)

    def test_as_msec(self):
        ts = TS("2022-12-07T00:00:00.123456Z")
        msec = ts.as_msec()
        self.assertIsInstance(msec, iTSms)
        self.assertEqual(msec, 1670371200123)

    def test_as_usec(self):
        ts = TS("2022-12-07T00:00:00.123456Z")
        usec = ts.as_usec()
        self.assertIsInstance(usec, iTSus)
        self.assertEqual(usec, 1670371200123456)

    def test_as_nsec(self):
        ts = TS("2022-12-07T00:00:00.123456789Z", prec="ns")
        nsec = ts.as_nsec()
        self.assertIsInstance(nsec, iTSns)
        self.assertEqual(nsec, 1670371200123457000)
        ts2 = TS(1670371200123456789, prec="ns")
        nsec2 = ts2.as_nsec()
        self.assertIsInstance(nsec2, iTSns)
        self.assertEqual(nsec2, 1670371200123457000)

    def test_as_ms_property_deprecated(self):
        ts = TS("2022-12-07T00:00:00.123456Z")
        ms = ts.as_ms
        self.assertIsInstance(ms, iTSms)
        self.assertEqual(ms, 1670371200123)

    def test_regression_with_MSec_diff(self):
        ts1 = TSMsec(0)
        ts2 = TSMsec(10)
        ts_diff = ts2 - ts1
        self.assertEqual(ts_diff, 0.010)
        ms_diff = ts_diff.as_ms
        self.assertEqual(ms_diff, 10)

    def test_sum_small_values(self):
        ts1 = TSMsec(0)
        ts2 = TSMsec(10)
        ts_sum = ts2 + ts1
        self.assertEqual(ts_sum, 0.010)
        ms_sum = ts_sum.as_ms
        self.assertEqual(ms_sum, 10)

    def test_radd_small_values(self):
        ts1 = TSMsec(0)
        ts_sum = 0.010 + ts1
        self.assertEqual(ts_sum, 0.010)
        ms_sum = ts_sum.as_ms
        self.assertEqual(ms_sum, 10)

    def test_sec_diff(self):
        ts1 = TS(0)
        ts2 = TS(10)
        ts_diff = ts2 - ts1
        self.assertEqual(ts_diff, 10)
        ms_diff = ts_diff.as_ms
        self.assertEqual(ms_diff, 10000)

    def test_rsub_small_values(self):
        ts1 = TSMsec(0)
        ts_sum = 0.010 - ts1
        self.assertEqual(ts_sum, 0.010)
        ms_sum = ts_sum.as_ms
        self.assertEqual(ms_sum, 10)

    def test_ts_pickle_roundtrip(self):
        ts = TS("2022-12-07T00:00:00.123456Z")
        ts2 = pickle.loads(pickle.dumps(ts))
        self.assertEqual(ts, ts2)

    def test_tsms_pickle_roundtrip(self):
        ts_ms = TSMsec("2022-12-07T00:00:00.123456Z")
        ts_ms2 = pickle.loads(pickle.dumps(ts_ms))
        self.assertEqual(ts_ms, ts_ms2)


class TestBaseTS(TestCase):
    def test_hash(self):
        """
        The hash of all the timestamp classes should be the same for the same timestamp,
        no matter the precision. All of them are reduced to the ns before hashing.
        """
        ts = TS("2018-02-28T01:01:01.123Z")
        d = {ts: 1}
        ts2 = TSMsec("2018-02-28T01:01:01.123Z")
        self.assertEqual(d[ts2], 1)
        f = float(ts)
        ts3 = TS(f)
        self.assertEqual(d[ts3], 1)
        its_ms = iTSms(ts)
        self.assertEqual(its_ms, ts)
        self.assertEqual(d[its_ms], 1)

        self.assertEqual(its_ms, ts2)
        its2 = iTSms(ts3)
        self.assertEqual(d[its2], 1)
        its_us = iTSus(ts)
        self.assertEqual(d[its_us], 1)
        its_ns = iTSns(ts)
        self.assertEqual(d[its_ns], 1)

        ts = TS("2018-02-28T01:01:01")
        d_sec = {ts: 1}
        self.assertEqual(d_sec[iTS(ts)], 1)

    def test_timestamp(self):
        ts = TS(1519855200.123)
        self.assertIsInstance(ts.timestamp(), float)
        self.assertEqual(ts.timestamp(), 1519855200.123)

    def test_from_ints(self):
        ts = iTS.from_parts_utc(2022, 12, 7, 1, 2, 3, 999, 999, 999)
        self.assertEqual(ts, iTS("2022-12-07T01:02:03Z"))

        ts = iTS.from_parts_utc(2022, 12, 7, 1, 2, 3, 999, 999, 999)
        self.assertEqual(ts, iTS("2022-12-07T01:02:03"))

        ts = iTSms.from_parts_utc(2022, 12, 7, 1, 2, 3, 456, 999, 999)
        self.assertEqual(ts, iTSms("2022-12-07T01:02:03.456Z"))

        ts = iTSus.from_parts_utc(2022, 12, 7, 1, 2, 3, 456, 789, 999)
        self.assertEqual(ts, iTSus("2022-12-07T01:02:03.456789Z"))

        ts = iTSns.from_parts_utc(2022, 12, 7, 1, 2, 3, 456, 789, 999)
        self.assertEqual(ts, iTSns("2022-12-07T01:02:03.456789999Z"))

        ts = TS.from_parts_utc(2022, 12, 7, 1, 2, 3, 456, 789, 999)
        self.assertEqual(ts, TS("2022-12-07T01:02:03.456790Z"))  # we don't have enough float precision to represent nanos

        ts = TS.from_parts_utc(2022, 12, 7, 1, 2, 3, 456, 789, 123)
        self.assertEqual(ts, TS("2022-12-07T01:02:03.456789Z"))  # we don't have enough float precision to represent nanos

        ts = TSMsec.from_parts_utc(2022, 12, 7, 1, 2, 3, 456, 789, 123)
        self.assertEqual(ts, TSMsec("2022-12-07T01:02:03.456789Z"))  # we don't have enough float precision to represent nanos
        ts = TSMsec.from_parts_utc(2022, 12, 7, 1, 2, 3, 456)
        self.assertEqual(ts, TSMsec("2022-12-07T01:02:03.456Z"))
        ts = TS.from_parts_utc(2022, 12, 7, 1, 2, 3)
        self.assertEqual(ts, TS("2022-12-07T01:02:03Z"))

    def test_comparisons(self):
        ts1 = TS("2022-12-07T00:00:00.000001Z")
        ts2 = TS("2022-12-07T00:00:00.000002Z")
        its1 = iTS("2022-12-07T00:00:00Z")
        its2 = iTS("2022-12-07T00:00:01Z")
        self.assertNotEqual(ts1, ts2)
        self.assertLess(ts1, ts2)
        self.assertGreater(ts2, ts1)
        self.assertLess(its1, its2)
        self.assertGreater(its2, its1)
        self.assertLess(ts1, its2)
        self.assertGreater(its2, ts1)
        self.assertLess(its1, ts2)
        self.assertGreater(ts2, its1)

        itsms1 = iTSms("2022-12-07T00:00:00.001Z")
        itsms2 = iTSms("2022-12-07T00:00:00.002Z")
        self.assertNotEqual(itsms1, itsms2)
        self.assertLess(itsms1, itsms2)
        self.assertGreater(itsms2, itsms1)
        self.assertGreater(itsms1, ts2)
        self.assertLess(ts2, itsms1)

        itsus1 = iTSus("2022-12-07T00:00:00.000001Z")
        itsus2 = iTSus("2022-12-07T00:00:00.000002Z")
        self.assertNotEqual(itsus1, itsus2)
        self.assertLess(itsus1, itsus2)
        self.assertGreater(itsus2, itsus1)
        self.assertEqual(itsus1, ts1)
        self.assertNotEqual(itsus1, ts2)
        self.assertLess(itsus1, ts2)
        self.assertGreater(ts2, itsus1)
        self.assertGreater(itsus2, ts1)
        self.assertTrue(itsus1 < int(ts2.as_usec()))
        self.assertTrue(int(ts2.as_usec()) > itsus1)
        self.assertTrue(itsus1 < float(ts2.as_usec()))
        self.assertTrue(float(ts2.as_usec()) > itsus1)

        itsns0 = iTSns("2022-12-07T00:00:00.000000999Z")
        itsns1 = iTSns("2022-12-07T00:00:00.000001001Z")
        itsns2 = iTSns("2022-12-07T00:00:00.000001002Z")
        self.assertNotEqual(itsns1, itsns2)
        self.assertLess(itsns1, itsns2)
        self.assertGreater(itsns2, itsns1)
        self.assertGreater(itsns1, ts1)
        self.assertLess(itsns0, ts1)
        self.assertGreater(ts1, itsns0)
        self.assertNotEqual(itsns1, itsus1)

        self.assertTrue(itsns1 < int(ts2.as_nsec()))
        self.assertTrue(int(ts2.as_nsec()) > itsns1)
        i = int(ts1.as_nsec())
        self.assertTrue(i < int(itsns1))
        self.assertTrue(i != itsns1)
        self.assertTrue(itsns1 != i)
        self.assertTrue(i < itsns1)
        self.assertTrue(itsns1 > int(ts1.as_nsec()))
        self.assertTrue(int(ts1.as_nsec()) < itsns1)
        self.assertTrue(itsns1 < float(ts2.as_nsec()))
        self.assertTrue(float(ts2.as_nsec()) > itsns1)
        self.assertTrue(itsns1 < ts2.as_nsec())
        self.assertTrue(ts2.as_nsec() > itsns1)
        self.assertTrue(itsns1 < ts2)
        self.assertTrue(ts2 > itsns1)


class Test_iTS(TestCase):
    def test_itsms_formatting(self):
        ts_str = "2025-09-08T02:17:29.981Z"
        parsed = iTSms(ts_str)
        self.assertEqual(1757297849981, int(parsed))

        as_iso = parsed.isoformat()
        self.assertEqual(ts_str, as_iso)
        self.assertEqual(ts_str, str(parsed))  # useless since it's calling .isoformat()

    def test_from_iso(self):
        run_test_from_iso(self, iTS)

    def test_iTS_constructors(self):
        ts = TS(1519855200.567)
        its = iTS(ts)
        self.assertIsInstance(its, iTS)
        self.assertEqual(its, 1519855201)
        its = iTS(1519855200.567)
        self.assertIsInstance(its, iTS)
        self.assertEqual(its, 1519855201)
        its = iTS(1519855201)
        self.assertIsInstance(its, iTS)
        self.assertEqual(its, 1519855201)
        its = iTS("1519855201.123")
        self.assertIsInstance(its, iTS)
        self.assertEqual(its, 1519855201)
        its = iTS("20120228-220000Z")
        self.assertIsInstance(its, iTS)
        self.assertEqual(its, 1330466400)

        its = iTS("20120228-220000")
        new_ts = iTS(its)
        self.assertIsInstance(new_ts, iTS)
        self.assertEqual(new_ts, 1330466400)

    def test_from_iTS(self):
        ts = iTS(1519855200.567)
        its = iTS(ts)
        self.assertIsInstance(its, iTS)
        self.assertEqual(its, 1519855201)

    def test_from_iTSms(self):
        ts = iTSms(1519855200600)
        its = iTS(ts)
        self.assertIsInstance(its, iTS)
        self.assertEqual(its, 1519855201)

    def test_from_iTSus(self):
        ts = iTSus(1519855200600000)
        its = iTS(ts)
        self.assertIsInstance(its, iTS)
        self.assertEqual(its, 1519855201)

    def test_from_iTSns(self):
        ts = iTSns(1519855200600000000.567)
        its = iTS(ts)
        self.assertIsInstance(its, iTS)
        self.assertEqual(its, 1519855201)

    def test_from_float_str(self):
        ts = iTS(ts="1519855200.123856")
        self.assertEqual(ts, 1519855200)

    def test_from_int_str(self):
        ts = iTS(ts="1519855200")
        self.assertEqual(ts, 1519855200)

    def test_as_iso(self):
        ts = iTS(ts=TestTS.INT_BASE_TS)
        self.assertEqual(ts.isoformat(), TestTS.STR_SEC_TS)
        ts = iTS(ts=TestTS.INT_BASE_TS) + 1
        self.assertEqual(ts.isoformat(), "2018-02-28T22:00:01Z")

    def test_from_reduced_iso_str_through_date_util_parser_explicit_TZ_no_utc(self):
        ts = iTS("20221202235700Z", utc=False)
        expected_dt_utc = datetime.fromisoformat("2022-12-02T23:57:00").replace(tzinfo=timezone.utc)
        expected_ts_utc = expected_dt_utc.timestamp()
        self.assertEqual(ts, expected_ts_utc)

    def test_from_reduced_iso_str_through_date_util_parser_no_TZ_no_utc(self):
        ts = iTS("20221202235700", utc=False)
        expected_dt = datetime.fromisoformat("2022-12-02T23:57:00")
        expected_ts = expected_dt.timestamp()
        self.assertEqual(ts, expected_ts)

    def test_from_reduced_iso_str_through_date_util_parser_explicit_TZ_with_utc_flag(self):
        ts = iTS("20221202235700Z")
        expected_ts = iTS("2022-12-02T23:57:00Z")
        self.assertEqual(ts, expected_ts)

    def test_from_reduced_iso_str_through_date_util_parser_no_TZ_with_utc_flag(self):
        ts = iTS("20221202235700")
        expected_ts = iTS("2022-12-02T23:57:00Z")
        self.assertEqual(ts, expected_ts)

    def test_as_iso_tz_standard(self):
        ts = iTS("2018-03-01T00:00:00Z")
        res = ts.iso_tz(pytz.timezone("Europe/Bucharest"))
        self.assertEqual("2018-03-01T02:00:00+02:00", res)
        res = ts.iso_tz("Europe/Bucharest")
        self.assertEqual(res, "2018-03-01T02:00:00+02:00")

    def test_as_iso_tz_DST(self):
        ts = iTS("2020-06-01T10:00:00Z")
        res = ts.iso_tz(pytz.timezone("Europe/Bucharest"))
        self.assertEqual("2020-06-01T13:00:00+03:00", res)

    def test_default_utc(self):
        expected = iTS.from_iso("2022-12-07T00:00:00Z")
        ts = iTS("2022-12-07T00:00:00")
        self.assertEqual(expected, ts)
        ts = iTS("20221207")
        self.assertEqual(expected, ts)
        t = time()
        print(t)

    def test_local(self):
        unix_ts = int(time())
        lt = localtime(unix_ts)
        iso_str = strftime("%Y-%m-%dT%H:%M:%S", lt)
        local_ts = iTS(iso_str, utc=False)
        self.assertEqual(iTS(unix_ts), local_ts)

    def test_from_numpy_numbers(self):
        n = np.int64(1519855200)
        ts = iTS(ts=n)
        self.assertEqual(ts, 1519855200)
        ts = iTS(ts=np.float64(1519855200.567))
        self.assertEqual(ts, 1519855201)

    def test_floor(self):
        ts = iTS("2022-12-07T00:00:13.123456Z")
        self.assertEqual(ts.floor(1), 1670371213)
        self.assertEqual(ts.floor(2), 1670371212)
        self.assertEqual(ts.floor(60), 1670371200)

    def test_ceil(self):
        ts = iTS("2022-12-07T00:00:13.123456Z")
        self.assertEqual(ts.ceil(1), 1670371213)
        self.assertEqual(ts.ceil(2), 1670371214)
        self.assertEqual(ts.ceil(60), 1670371260)

    def test_math_ops(self):
        ts = iTS(ts=1519855200)
        self.assertIsInstance(ts + 20, iTS)
        self.assertEqual(ts + 20, 1519855220)
        self.assertIsInstance(20 + ts, iTS)
        self.assertEqual(20 + ts, 1519855220)
        self.assertIs(type(ts * 1), int)
        self.assertIs(type(1 * ts), int)
        self.assertIs(type(ts / 1), float)
        self.assertIs(type(100000 / ts), float)

    def test_repr(self):
        ts = iTS(ts=TestTS.INT_BASE_TS)
        self.assertEqual(repr(ts), "iTS('2018-02-28T22:00:00Z')")

    def test_str(self):
        ts = iTS(ts=TestTS.INT_BASE_TS)
        self.assertEqual(str(ts), '2018-02-28T22:00:00Z')

    def test_pydantic_validator(self):
        class TestModel(BaseModel):
            ts: iTS

        t1 = TestModel(ts="2022-12-07T00:00:00.123456Z")
        self.assertEqual(t1.ts, 1670371200)

        t1 = TestModel(ts=1670371200.123456)
        self.assertEqual(t1.ts, 1670371200)

        t1 = TestModel(ts=1670371200)
        self.assertEqual(t1.ts, 1670371200)

    def test_pickle_roundtrip(self):
        ts = iTS("2022-12-07T00:00:00.123456Z")
        ts2 = pickle.loads(pickle.dumps(ts))
        self.assertEqual(ts, ts2)

    def test_add_float(self):
        ts = iTS(10)
        ts2 = ts + 1.123456
        self.assertEqual(ts2, 11)
        self.assertIsInstance(ts2, iTS)

    def test_sub_float(self):
        ts = iTS(10)
        ts2 = ts - 1.123456
        self.assertEqual(ts2, 9)
        self.assertIsInstance(ts2, iTS)

    def test_sub_TS(self):
        ts = iTS(10)
        ts2 = ts - TS(1.123456)
        self.assertEqual(ts2, 9)
        self.assertIsInstance(ts2, iTS)


class Test_dTS(TestCase):
    def test_delta_round_float(self):
        ts = TS("2022-12-07T00:00:00.123456Z")
        ts_plus = ts + dTS(10.1)
        self.assertEqual(ts_plus, TS("2022-12-07T00:00:10.123456Z"))
        ts_plus = ts + dTS(10.9)
        self.assertEqual(ts_plus, TS("2022-12-07T00:00:11.123456Z"))

        ts_minus = ts - dTS(10.1)
        self.assertEqual(ts_minus, TS("2022-12-06T23:59:50.123456Z"))
        ts_minus = ts - dTS(10.9)
        self.assertEqual(ts_minus, TS("2022-12-06T23:59:49.123456Z"))

    def test_months_delta_last_day(self):
        ts = TS("2022-12-31T00:00:00.123456Z")
        dts = dTS("2M")
        ts_plus = ts + dts
        self.assertEqual(ts_plus, TS("2023-02-28T00:00:00.123456Z"))
        ts_minus = ts - dts
        self.assertEqual(ts_minus, TS("2022-10-31T00:00:00.123456Z"))

        ts_feb = TS("2022-02-28T00:00:00.123456Z")
        ts_plus = ts_feb + dts
        self.assertEqual(ts_plus, TS("2022-04-28T00:00:00.123456Z"))
        ts_minus = ts_feb - dts
        self.assertEqual(ts_minus, TS("2021-12-28T00:00:00.123456Z"))

    def test_months_delta_first_day(self):
        ts = TS("2022-01-01T00:00:00.123456Z")
        dts = dTS("2M")
        ts_plus = ts + dts
        self.assertEqual(ts_plus, TS("2022-03-01T00:00:00.123456Z"))
        ts_minus = ts - dts
        self.assertEqual(ts_minus, TS("2021-11-01T00:00:00.123456Z"))

    def test_years_delta_last_month_day(self):
        # Leap year
        ts = TS("2024-02-29T00:00:00.123456Z")
        dts = dTS("2Y")
        ts_plus = ts + dts
        self.assertEqual(ts_plus, TS("2026-02-28T00:00:00.123456Z"))
        ts_minus = ts - dts
        self.assertEqual(ts_minus, TS("2022-02-28T00:00:00.123456Z"))

        # non-leap year
        ts = TS("2022-02-28T00:00:00.123456Z")
        ts_plus = ts + dts
        # this is leap year, but we don't care of the last day of the month
        self.assertEqual(ts_plus, TS("2024-02-28T00:00:00.123456Z"))
        ts_minus = ts - dts
        self.assertEqual(ts_minus, TS("2020-02-28T00:00:00.123456Z"))

    def test_encode_decode(self):
        dts = dTS(5 * 60 * 1_000_000_000, unit='ns')
        self.assertEqual(str(dts), "5m")
        dts = dTS(60000, "ms")
        self.assertEqual(str(dts), "1m")
        self.assertEqual(repr(dts), 'dTS("1m")')
        dts = dTS("300000ms")
        self.assertEqual(str(dts), "5m")
        dts = dTS("7200000ms")
        self.assertEqual(str(dts), "2h")
        dts = dTS("-7200000ms")
        self.assertEqual(str(dts), "-2h")
        dts = dTS(2 * 24 * 3600)
        self.assertEqual(str(dts), "2d")
        dts = dTS(2 * 7 * 24 * 3600)
        self.assertEqual(str(dts), "2w")

        dts = dTS(2, unit="Y")
        self.assertEqual(str(dts), "2Y")
        dts = dTS(2 * 12, unit="M")
        self.assertEqual(str(dts), "2Y")
        dts = dTS(13, unit="M")
        self.assertEqual(str(dts), "13M")

    def test_equality(self):
        dts = dTS(5 * 60 * 1_000_000_000, unit='ns')
        self.assertEqual(dts, dTS("5m"))
        self.assertEqual(dts, dTS("300000ms"))
        self.assertEqual(dTS('2Y'), dTS("24M"))

        self.assertNotEqual(dts, dTS("300001ms"))

    def test_invalid_input(self):
        with self.assertRaises(ValueError):
            dTS("1")
        with self.assertRaises(ValueError):
            dTS("1 s")
        with self.assertRaises(ValueError):
            dTS("+1s")
        with self.assertRaises(ValueError):
            dTS("1ps")


class Test_iTSms(TestCase):
    def test_from_iso(self):
        run_test_from_iso(self, iTSms)

    def test_error_message_regression(self):
        a = "invalid-timestamp"
        with self.assertRaises(ValueError) as e:
            iTSms(a)
        self.assertIn(a, str(e.exception))

    def test_constructors(self):
        ts = TS(1519855200.567)
        its = iTSms(ts)
        self.assertIsInstance(its, iTSms)
        self.assertEqual(its, 1519855200567)
        its = iTSms("1519855201123.456")
        self.assertIsInstance(its, iTSms)
        self.assertEqual(its, 1519855201123)
        its = iTSms("2012-02-28T22:00:00.123456Z")
        self.assertIsInstance(its, iTSms)
        self.assertEqual(its, 1330466400123)

    def test_from_iTS(self):
        ts = iTS(ts=1519855200)
        ms = iTSms(ts)
        self.assertEqual(1519855200000, ms)

    def test_from_iTSms(self):
        ms = iTSms(ts=1519855200123)
        ms2 = iTSms(ms)
        self.assertEqual(ms, ms2)

    def test_from_iTSus(self):
        us = iTSus(ts=1519855200123456)
        ms = iTSms(us)
        self.assertEqual(1519855200123, ms)

    def test_from_iTSns(self):
        ns = iTSns(ts=1519855200123456789)
        ms = iTSms(ns)
        self.assertEqual(1519855200123, ms)

    def test_from_numpy_numbers(self):
        n = np.int64(1519855200000)
        ts = iTSms(ts=n)
        self.assertEqual(ts, 1519855200000)
        ts = iTSms(ts=np.float64(1519855200123.456))
        self.assertEqual(ts, 1519855200123)

    def test_floor(self):
        ts = iTSms("2022-12-07T00:00:00.999")
        self.assertEqual(ts.floor(1), 1670371200999)
        self.assertEqual(ts.floor(2), 1670371200998)
        self.assertEqual(ts.floor(1000), 1670371200000)

    def test_ceil(self):
        ts = iTSms("2022-12-07T00:00:00.001Z")
        self.assertEqual(ts.ceil(1), 1670371200001)
        self.assertEqual(ts.ceil(2), 1670371200002)
        self.assertEqual(ts.ceil(1000), 1670371201000)

    def test_math_ops(self):
        ts = iTSms(ts=1519855200000)
        self.assertIsInstance(ts + 20, iTSms)
        self.assertEqual(ts + 20, 1519855200020)
        self.assertIsInstance(20 + ts, iTSms)
        self.assertEqual(20 + ts, 1519855200020)
        self.assertIs(type(ts * 1), int)
        self.assertIs(type(1 * ts), int)
        self.assertIs(type(ts / 1), float)
        self.assertIs(type(100000 / ts), float)

    def test_repr(self):
        ts = iTSms(ts=TestTS.INT_BASE_MS_TS)
        self.assertEqual("iTSms('2018-02-28T22:00:00.123Z')", repr(ts))

    def test_str(self):
        ts = iTSms(ts=TestTS.INT_BASE_MS_TS)
        self.assertEqual(TestTS.STR_MSEC_TS, str(ts))

    def test_pydantic_validator(self):
        class TestModel(BaseModel):
            ts: iTSms

        t1 = TestModel(ts="2022-12-07T00:00:00.123456Z")
        self.assertEqual(t1.ts, 1670371200123)

        t1 = TestModel(ts=1670371200123.456)
        self.assertEqual(t1.ts, 1670371200123)

        t1 = TestModel(ts=1670371200123.000)
        self.assertEqual(t1.ts, 1670371200123)

    def test_diff_small_values(self):
        t1 = iTSms(0)
        t2 = iTSms(10)
        self.assertEqual(t2 - t1, 10)
        self.assertEqual(t1 - t2, -10)

    def test_pickle_roundtrip(self):
        ts = iTSms("2022-12-07T00:00:00.123456Z")
        ts2 = pickle.loads(pickle.dumps(ts))
        self.assertEqual(ts, ts2)

    def test_add_float(self):
        ts1 = iTSms(10)
        ts_sum = ts1 + 1.001
        self.assertIsInstance(ts_sum, iTSms)
        self.assertEqual(ts_sum, 11)

    def test_radd_float(self):
        ts1 = iTSms(10)
        ts_sum = 1.001 + ts1
        self.assertIsInstance(ts_sum, float)
        self.assertEqual(ts_sum, 11.001)

    def test_regression_sub_float(self):
        ts1 = iTSms(10)
        ts_diff = ts1 - 1.001
        self.assertIsInstance(ts_diff, iTSms)
        self.assertEqual(ts_diff, 9)

    def test_rsub_float(self):
        ts1 = iTSms(10)
        ts_diff = 10.001 - ts1
        self.assertIsInstance(ts_diff, float)
        self.assertAlmostEqual(ts_diff, 0.001)


class Test_iTSus(TestCase):
    def test_from_iso(self):
        run_test_from_iso(self, iTSus)

    def test_itsus_ms_regression(self):
        """ ensures that when the ts has ms precision, we don't add float imprecision/add extra digits when converting to ns """
        its_us = iTSus("2025-09-08T02:17:29.981", utc=False)
        dt = datetime.fromisoformat("2025-09-08T02:17:29.981")
        self.assertEqual(dt.timestamp(), its_us.timestamp())

        its_us = iTSus("2025-09-08T02:17:29.981")
        self.assertEqual(1757297849981_000, int(its_us))
        self.assertEqual("2025-09-08T02:17:29.981000Z", its_us.isoformat())

        its_us = iTSus("2025-09-08T02:17:29.981Z")
        self.assertEqual(1757297849981_000, int(its_us))
        self.assertEqual("2025-09-08T02:17:29.981000Z", its_us.isoformat())

    def test_constructors(self):
        ts = TS(1519855200.123456)
        its = iTSus(ts)
        self.assertIsInstance(its, iTSus)
        self.assertEqual(its, 1519855200123456)
        its = iTSus("1519855200123456.7")
        self.assertIsInstance(its, iTSus)
        self.assertEqual(its, 1519855200123457)
        its = iTSus("2012-02-28T22:00:00.123456Z")
        self.assertIsInstance(its, iTSus)
        self.assertEqual(its, 1330466400123456)
        # round up
        ts = iTSus(ts=1519855200000000.9)
        self.assertEqual(ts, 1519855200000001)
        # round down
        ts = iTSus(ts=1519855200000000.499)
        self.assertEqual(ts, 1519855200000000)

    def test_regression_constructor(self):
        with self.assertRaises(ValueError):
            ts = iTSus("2025-06-08T30:00:00Z")
        ts = iTSus("2025-06-08T23:00:00Z")
        self.assertEqual(ts, TS("2025-06-08T23:00:00Z"))

    def test_from_iTS(self):
        ts = iTS(ts=1519855200)
        us = iTSus(ts)
        self.assertEqual(1519855200000000, us)

    def test_from_iTSms(self):
        ms = iTSms(ts=1519855200123)
        us = iTSus(ms)
        self.assertEqual(1519855200123000, us)

    def test_from_iTSus(self):
        us = iTSus(ts=1519855200123456)
        us2 = iTSus(us)
        self.assertEqual(us, us2)

    def test_from_iTSns(self):
        ns = iTSns(ts=1519855200123456789)
        us = iTSus(ns)
        self.assertEqual(1519855200123457, us)

    def test_from_numpy_numbers(self):
        n = np.int64(1519855200000000)
        ts = iTSus(ts=n)
        self.assertEqual(ts, 1519855200000000)
        ts = iTSus(ts=np.float64(1519855200123456.789))
        self.assertEqual(ts, 1519855200123457)

    def test_floor(self):
        ts = iTSus("2022-12-07T00:00:00.999999")
        self.assertEqual(ts.floor(1), 1670371200999999)
        self.assertEqual(ts.floor(2), 1670371200999998)
        self.assertEqual(ts.floor(1000000), 1670371200000000)

    def test_ceil(self):
        ts = iTSus("2022-12-07T00:00:00.000001Z")
        self.assertEqual(ts.ceil(1), 1670371200000001)
        self.assertEqual(ts.ceil(2), 1670371200000002)
        self.assertEqual(ts.ceil(1000000), 1670371201000000)

    def test_math_ops(self):
        ts = iTSus(ts=1519855200000000)
        self.assertIsInstance(ts + 20, iTSus)
        self.assertEqual(ts + 20, 1519855200000020)
        self.assertIsInstance(20 + ts, iTSus)
        self.assertEqual(20 + ts, 1519855200000020)
        self.assertIs(type(ts * 1), int)
        self.assertIs(type(1 * ts), int)
        self.assertIs(type(ts / 1), float)
        self.assertIs(type(100000 / ts), float)

    def test_repr(self):
        ts = iTSus(ts=1519855200123456)
        self.assertEqual(repr(ts), "iTSus('2018-02-28T22:00:00.123456Z')")

    def test_str(self):
        ts = iTSus(ts=1519855200123456)
        self.assertEqual(str(ts), "2018-02-28T22:00:00.123456Z")

    def test_pickle_roundtrip(self):
        ts = iTSus("2022-12-07T00:00:00.123456Z")
        ts2 = pickle.loads(pickle.dumps(ts))
        self.assertEqual(ts, ts2)


class Test_iTSns(TestCase):
    def test_from_iso(self):
        run_test_from_iso(self, iTSns)

    def test_regression_itsns_constructor_vs_from_iso(self):
        """Test that iTSns() and iTSns.from_iso() produce the same result"""
        date_str = "2025-11-02T13:45:14.012345678Z"
        result1 = iTSns(date_str)
        result2 = iTSns.from_iso(date_str)
        self.assertEqual(result1, result2)
        self.assertEqual( 1762091114012345678, int(result1))
        self.assertEqual(result2.isoformat(), date_str)

    def test_regression_from_basic_iso(self):
        ts_str = "20251007T131121.098321827Z"
        its = iTSns.from_iso(ts_str)
        self.assertEqual("2025-10-07T13:11:21.098321827Z", its.isoformat())
        self.assertEqual(ts_str, its.iso_basic(sep="T"))
        self.assertEqual("20251007T131121.098321827", its.iso_basic(sep="T", use_zulu=False))
        self.assertEqual("20251007-131121.098321827Z", its.iso_basic())
        self.assertEqual("20251007-131121.098321827", its.iso_basic(use_zulu=False))
        its = iTSns(ts_str)
        self.assertEqual(its.iso_basic(sep="T"), ts_str)

    def test_error_message_regression(self):
        a = "invalid-timestamp"
        with self.assertRaises(ValueError) as e:
            iTSns(a)
        self.assertIn(a, str(e.exception))

    def test_itsns_ms_regression(self):
        """ ensures that when the ts has ms precision, we don't add float imprecision/add extra digits when converting to ns """
        its_ns = iTSns("2025-09-08T02:17:29.981", utc=False)
        dt = datetime.fromisoformat("2025-09-08T02:17:29.981")
        self.assertEqual(dt.timestamp(), its_ns.timestamp())

        its_ns = iTSns("2025-09-08T02:17:29.981")
        self.assertEqual(1757297849981_000_000, int(its_ns))
        self.assertEqual("2025-09-08T02:17:29.981000000Z", its_ns.isoformat())

        its_ns = iTSns("2025-09-08T02:17:29.981Z")
        self.assertEqual(1757297849981_000_000, int(its_ns))
        self.assertEqual("2025-09-08T02:17:29.981000000Z", its_ns.isoformat())

    def test_constructors(self):
        ts = TS(1519855200.123456789)
        its = iTSns(ts)
        self.assertIsInstance(its, iTSns)
        self.assertEqual(its, 1519855200123457000)

        its = iTSns("20240101")
        self.assertIsInstance(its, iTSns)
        self.assertEqual(int(its), iTSns("2024-01-01T00:00:00.000000000Z"))
        its = iTSns(1519855200123456789)
        self.assertIsInstance(its, iTSns)
        self.assertEqual(its, 1519855200123456789)
        its = iTSns("2012-02-28T22:00:00.123456789Z")
        self.assertIsInstance(its, iTSns)
        self.assertEqual(its, 1330466400123456789)
        # round up
        ts = iTSns(ts=1519855200000000000.501)
        # ToDo: decide how to handle this case
        # self.assertEqual(ts, 1519855200000000001)
        # round down
        ts = iTSns(ts=1519855200000000000.499)
        # self.assertEqual(ts, 1519855200000000000)

    def test_constructor_with_no_tz_no_utc(self):
        ts_ms_gran = int(TS("2022-12-07T10:00:00.123456", utc=False).as_msec()) * 1_000_000
        its_us_gran = int(iTSns("2022-12-07T10:00:00.123456", utc=False))
        its_ns_gran = int(iTSns("2022-12-07T10:00:00.123456789", utc=False))
        self.assertEqual(ts_ms_gran + 456000, its_us_gran)
        self.assertEqual(its_us_gran + 789, its_ns_gran)

    def test_constructor_with_tz_no_utc(self):
        ts_ms_gran = int(TS("2022-12-07T10:00:00.123456+04", utc=False).as_msec()) * 1_000_000
        its_us_gran = int(iTSns("2022-12-07T10:00:00.123456+04", utc=False))
        its_ns_gran = int(iTSns("2022-12-07T10:00:00.123456789+04", utc=False))
        self.assertEqual(ts_ms_gran + 456000, its_us_gran)
        self.assertEqual(its_us_gran + 789, its_ns_gran)

    def test_constructor_with_utc_and_tz(self):
        ts_ms_gran = int(TS("2022-12-07T10:00:00.123456+04", utc=True).as_msec()) * 1_000_000
        its_us_gran = int(iTSns("2022-12-07T10:00:00.123456+04", utc=True))
        its_ns_gran = int(iTSns("2022-12-07T10:00:00.123456789+04", utc=True))
        self.assertEqual(ts_ms_gran + 456000, its_us_gran)
        self.assertEqual(its_us_gran + 789, its_ns_gran)

    def test_constructor_with_utc_no_tz(self):
        ts_ms_gran = int(TS("2022-12-07T10:00:00.123456Z", utc=True).as_msec()) * 1_000_000
        its_us_gran = int(iTSns("2022-12-07T10:00:00.123456Z", utc=True))
        its_ns_gran = int(iTSns("2022-12-07T10:00:00.123456789Z", utc=True))
        self.assertEqual(ts_ms_gran + 456000, its_us_gran)
        self.assertEqual(its_us_gran + 789, its_ns_gran)

    def test_regression_constructor(self):
        with self.assertRaises(ValueError):
            iTSns("2025-06-08T30:00:00Z")
        ts = iTSns("2025-06-08T23:00:00Z")
        self.assertEqual(ts, TS("2025-06-08T23:00:00"))
        # constructor from iTS
        its1 = iTS("2025-06-08T23:00:00Z")
        itsns1 = iTSns(its1)
        self.assertEqual(itsns1, its1)

        its_less_digits = iTSns("2025-06-08T23:00:00.123456Z")
        self.assertEqual(int(its_less_digits) % 1_000_000_000, 123456000)

        itsus1 = iTSus(its1)
        self.assertEqual(itsus1, its1)

        itsms1 = iTSms(its1)
        self.assertEqual(itsms1, its1)

    def test_from_ns(self):
        # these big floats are not supported by python
        ts = iTSns(ts=1519855200000000000.0)
        self.assertEqual(ts, 1519855200000000000)
        # round down
        ts = iTSns(ts=1519855200000000000)
        self.assertEqual(ts, 1519855200000000000)
        ts = iTSns(Decimal(1519855200000000000))
        self.assertEqual(ts, 1519855200000000000)

    def test_from_numpy_numbers(self):
        n = np.int64(1519855200000000000)
        ts = iTSns(ts=n)
        self.assertEqual(ts, 1519855200000000000)
        ts = iTSns(ts=np.float64(1519855200000000000.1))
        self.assertEqual(ts, 1519855200000000000)

    def test_floor(self):
        ts = iTSns(1670371200999999999)
        self.assertEqual(ts.floor(1), 1670371200999999999)
        self.assertEqual(ts.floor(2), 1670371200999999998)
        self.assertEqual(ts.floor(1000000000), 1670371200000000000)

    def test_ceil(self):
        ts = iTSns(1670371200000000001)
        self.assertEqual(ts.ceil(1), 1670371200000000001)
        self.assertEqual(ts.ceil(2), 1670371200000000002)
        self.assertEqual(ts.ceil(1000000000), 1670371201000000000)

    def test_math_ops(self):
        ts = iTSns(ts=1519855200000000000)
        self.assertIsInstance(ts + 20, iTSns)
        self.assertEqual(ts + 20, 1519855200000000020)
        self.assertIsInstance(20 + ts, iTSns)
        self.assertEqual(20 + ts, 1519855200000000020)
        self.assertIs(type(ts * 1), int)
        self.assertIs(type(1 * ts), int)
        self.assertIs(type(ts / 1), float)
        self.assertIs(type(1000000000 / ts), float)

    def test_repr(self):
        ts = iTSns(ts=1519855200123456789)
        self.assertEqual(repr(ts), "iTSns('2018-02-28T22:00:00.123456789Z')")

    def test_str(self):
        ts = iTSns(ts=1519855200123456789)
        # rounded up to microseconds
        self.assertEqual(str(ts), "2018-02-28T22:00:00.123456789Z")

    def test_isoformat(self):
        ts = iTSns(ts=1519855200123456789)
        self.assertEqual(ts.isoformat(), "2018-02-28T22:00:00.123456789Z")
        self.assertEqual(ts.isoformat(sep=' '), "2018-02-28 22:00:00.123456789Z")
        self.assertEqual(ts.isoformat(sep='T', timespec='microseconds'), "2018-02-28T22:00:00.123457Z")
        self.assertEqual(ts.isoformat(sep='T', timespec='seconds'), "2018-02-28T22:00:00Z")

    def test_now(self):
        now_ts = iTSns.now()
        ns_ts = time_ns()
        sec_ts = time()
        its = iTSns(sec_ts * 1_000_000_000)
        print(int(ns_ts - now_ts))
        print(int(now_ts - its))

        self.assertLess(ns_ts - now_ts, 1_000_000)
        self.assertLess(abs(its - now_ts), 50_000)

    def test_pickle_roundtrip(self):
        ts = iTSns("2022-12-07T00:00:00.123456Z")
        ts2 = pickle.loads(pickle.dumps(ts))
        self.assertEqual(ts, ts2)

    def test_user_warning(self):
        ts_str = "1970-01-01T00:00:00.100000000Z"

        i_ns = BaseTS.ns_timestamp_from_iso(ts_str[:-1], utc=True)
        self.assertEqual(i_ns, 100000000)

        i_ns = BaseTS.ns_timestamp_from_iso(ts_str, utc=False)
        self.assertEqual(i_ns, 100000000)

        i_ns = BaseTS.ns_timestamp_from_iso(ts_str, utc=True)
        self.assertEqual(i_ns, 100000000)

        self.assertEqual( 0, BaseTS.ns_timestamp_from_iso("1970-01-01T00:00:00", utc=False))
        self.assertEqual(0, BaseTS.ns_timestamp_from_iso("1970-01-01T00:00:00"))
        self.assertEqual(0, BaseTS.ns_timestamp_from_iso("1970-01-01T00:00:00Z", utc=False))
        self.assertEqual(0, BaseTS.ns_timestamp_from_iso("1970-01-01T00:00:00Z"))



class TestTimedeltaOps(TestCase):
    """Comprehensive tests for timedelta arithmetic operations across all timestamp classes."""

    # Test data for parametrized tests
    TIMEDELTA_TEST_CASES = [
        # (timedelta_kwargs, description)
        ({'days': 1}, 'single_day'),
        ({'days': -1}, 'negative_day'),
        ({'days': 1.5}, 'fractional_days'),
        ({'seconds': 1}, 'single_second'),
        ({'seconds': -1}, 'negative_second'),
        ({'seconds': 1.5}, 'fractional_seconds'),
        ({'seconds': 0.001}, 'millisecond_precision'),
        ({'seconds': 0.000001}, 'microsecond_precision'),
        ({'milliseconds': 1}, 'single_millisecond'),
        ({'milliseconds': -1}, 'negative_millisecond'),
        ({'milliseconds': 1.5}, 'fractional_milliseconds'),
        ({'microseconds': 1}, 'single_microsecond'),
        ({'microseconds': -1}, 'negative_microsecond'),
        ({'microseconds': 1.5}, 'fractional_microseconds'),
        ({'hours': 1}, 'single_hour'),
        ({'hours': -1}, 'negative_hour'),
        ({'minutes': 1}, 'single_minute'),
        ({'minutes': -1}, 'negative_minute'),
        ({'weeks': 1}, 'single_week'),
        ({'weeks': -1}, 'negative_week'),
        # Complex combinations
        ({'days': 1, 'hours': 2, 'minutes': 3, 'seconds': 4.5}, 'complex_positive'),
        ({'days': -1, 'hours': -2, 'minutes': -3, 'seconds': -4.5}, 'complex_negative'),
        ({'days': 1, 'seconds': -3600}, 'mixed_signs'),
    ]

    def _test_ts_arithmetic_parametrized(self, ts_class, base_value, test_cases):
        """Helper method for parametrized testing of timestamp arithmetic."""
        base = ts_class(base_value)

        # Define precision expectations for each timestamp class
        precision_config = {
            TS:    {'places': 9, 'tolerance': 0},  # Float precision
            iTS:   {'places': 0, 'tolerance': 0.5},  # Second precision (±0.5s rounding)
            iTSms: {'places': 3, 'tolerance': 0.001},  # Millisecond precision (±1ms for fractional rounding)
            iTSus: {'places': 6, 'tolerance': 0.000001},  # Microsecond precision (±1μs for fractional rounding)
            iTSns: {'places': 6, 'tolerance': 0.000001},  # Nanosecond precision limited by timedelta's microsecond resolution + float precision
        }

        config = precision_config[ts_class]

        for td_kwargs, description in test_cases:
            with self.subTest(ts_class=ts_class.__name__, case=description, kwargs=td_kwargs):
                td = timedelta(**td_kwargs)

                # Test addition
                result_add = base + td
                self.assertIsInstance(result_add, ts_class, f"Addition result should be {ts_class.__name__}")

                # Verify time correctness with appropriate precision for each class
                base_seconds = float(base.timestamp())
                td_seconds = td.total_seconds()
                expected_seconds = base_seconds + td_seconds
                actual_seconds = float(result_add.timestamp())

                if ts_class == TS:
                    # Float-based: expect high precision
                    self.assertAlmostEqual(actual_seconds, expected_seconds, places=config['places'],
                                           msg=f"Time correctness failed for {description}: expected {expected_seconds}, got {actual_seconds}")
                else:
                    # Integer-based: account for rounding to class precision
                    diff = abs(actual_seconds - expected_seconds)
                    self.assertLessEqual(diff, config['tolerance'],
                                         msg=f"Time correctness failed for {description}: expected {expected_seconds}, got {actual_seconds}, diff {diff} > tolerance {config['tolerance']}")

                # Test reverse addition
                result_radd = td + base
                self.assertIsInstance(result_radd, ts_class, f"Reverse addition result should be {ts_class.__name__}")
                self.assertEqual(result_add, result_radd, f"Addition and reverse addition should be equal")

                # Test subtraction
                result_sub = base - td
                self.assertIsInstance(result_sub, ts_class, f"Subtraction result should be {ts_class.__name__}")

                # Verify subtraction correctness
                expected_sub_seconds = base_seconds - td_seconds
                actual_sub_seconds = float(result_sub.timestamp())

                if ts_class == TS:
                    self.assertAlmostEqual(actual_sub_seconds, expected_sub_seconds, places=config['places'],
                                           msg=f"Subtraction correctness failed for {description}")
                else:
                    diff_sub = abs(actual_sub_seconds - expected_sub_seconds)
                    self.assertLessEqual(diff_sub, config['tolerance'],
                                         msg=f"Subtraction correctness failed for {description}: diff {diff_sub} > tolerance {config['tolerance']}")

                # Test reverse subtraction
                result_rsub = td - base
                self.assertIsInstance(result_rsub, ts_class, f"Reverse subtraction result should be {ts_class.__name__}")

                # Verify arithmetic consistency: (base + td) - td == base
                if ts_class in [TS]:  # Float-based classes
                    self.assertAlmostEqual(float((base + td) - td), float(base), places=9,
                                           msg=f"Arithmetic consistency failed for {description}")
                else:  # Integer-based classes - allow for rounding differences
                    diff = abs(int((base + td) - td) - int(base))
                    self.assertLessEqual(diff, 1,
                                         msg=f"Arithmetic consistency failed for {description} (diff: {diff})")

    def test_TS_timedelta_comprehensive(self):
        """Comprehensive parametrized tests for TS class."""
        self._test_ts_arithmetic_parametrized(TS, "2018-02-28T22:00:00Z", self.TIMEDELTA_TEST_CASES)

    def test_iTS_timedelta_comprehensive(self):
        """Comprehensive parametrized tests for iTS class."""
        self._test_ts_arithmetic_parametrized(iTS, "2018-02-28T22:00:00Z", self.TIMEDELTA_TEST_CASES)

    def test_iTSms_timedelta_comprehensive(self):
        """Comprehensive parametrized tests for iTSms class."""
        self._test_ts_arithmetic_parametrized(iTSms, "2018-02-28T22:00:00Z", self.TIMEDELTA_TEST_CASES)

    def test_iTSus_timedelta_comprehensive(self):
        """Comprehensive parametrized tests for iTSus class."""
        self._test_ts_arithmetic_parametrized(iTSus, "2018-02-28T22:00:00Z", self.TIMEDELTA_TEST_CASES)

    def test_iTSns_timedelta_comprehensive(self):
        """Comprehensive parametrized tests for iTSns class."""
        self._test_ts_arithmetic_parametrized(iTSns, "2018-02-28T22:00:00Z", self.TIMEDELTA_TEST_CASES)

    def test_timedelta_precision_edge_cases(self):
        """Test precision handling and edge cases for different timestamp classes."""

        # Test rounding behavior for integer classes
        base_its = iTS(10)
        self.assertEqual(base_its + timedelta(seconds=0.4), iTS(10), "iTS should round 0.4s to 0")
        self.assertEqual(base_its + timedelta(seconds=0.5), iTS(10), "iTS should round 0.5s to 0 (banker's rounding)")
        self.assertEqual(base_its + timedelta(seconds=0.6), iTS(11), "iTS should round 0.6s to 1")
        self.assertEqual(base_its + timedelta(seconds=1.5), iTS(12), "iTS should round 1.5s to 2")

        base_itsms = iTSms(1000)
        self.assertEqual(base_itsms + timedelta(milliseconds=0.4), iTSms(1000), "iTSms should round 0.4ms to 0")
        self.assertEqual(base_itsms + timedelta(milliseconds=0.5), iTSms(1000), "iTSms should round 0.5ms to 0")
        self.assertEqual(base_itsms + timedelta(milliseconds=0.6), iTSms(1001), "iTSms should round 0.6ms to 1")

        base_itsus = iTSus(1000)
        self.assertEqual(base_itsus + timedelta(microseconds=0.4), iTSus(1000), "iTSus should round 0.4μs to 0")
        self.assertEqual(base_itsus + timedelta(microseconds=0.5), iTSus(1000), "iTSus should round 0.5μs to 0")
        self.assertEqual(base_itsus + timedelta(microseconds=0.6), iTSus(1001), "iTSus should round 0.6μs to 1")

        # Test nanosecond precision - timedelta only supports microsecond precision (1000ns)
        base_itsns = iTSns(1000)
        # 1 microsecond = 1000 nanoseconds
        self.assertEqual(base_itsns + timedelta(microseconds=1), iTSns(2000))
        # Test with whole microseconds for reliable precision
        self.assertEqual(base_itsns + timedelta(microseconds=2), iTSns(3000))
        # timedelta minimum resolution is 1 microsecond, so smaller values are not supported

    def test_timedelta_zero_operations(self):
        """Test operations with zero timedelta."""
        zero_td = timedelta(0)

        for ts_class in [TS, iTS, iTSms, iTSus, iTSns]:
            with self.subTest(ts_class=ts_class.__name__):
                base = ts_class("2018-02-28T22:00:00Z")

                # Adding/subtracting zero should return equivalent timestamp
                self.assertEqual(base + zero_td, base)
                self.assertEqual(zero_td + base, base)
                self.assertEqual(base - zero_td, base)

                # zero - base should equal -base (in seconds)
                if ts_class == TS:
                    self.assertEqual(zero_td - base, TS(-float(base)))
                else:
                    expected_val = -int(base.timestamp() * ts_class.UNITS_IN_SEC)
                    self.assertEqual(zero_td - base, ts_class(expected_val))

    def test_timedelta_large_values(self):
        """Test operations with large timedelta values."""
        large_positive = timedelta(days=365 * 100)  # 100 years
        large_negative = timedelta(days=-365 * 100)  # -100 years

        base = TS("2018-02-28T22:00:00Z")

        # Test large positive
        result_pos = base + large_positive
        self.assertIsInstance(result_pos, TS)
        self.assertGreater(result_pos, base)

        # Test large negative
        result_neg = base + large_negative
        self.assertIsInstance(result_neg, TS)
        self.assertLess(result_neg, base)

        # Test reverse operations
        self.assertEqual(large_positive + base, result_pos)
        self.assertEqual(large_negative + base, result_neg)

    def test_timedelta_fractional_precision(self):
        """Test fractional precision handling across different units."""
        base = TS("2018-02-28T22:00:00Z")

        # Test fractional days
        result = base + timedelta(days=1.5)
        expected = base + timedelta(hours=36)
        self.assertEqual(result, expected)

        # Test fractional hours
        result = base + timedelta(hours=2.5)
        expected = base + timedelta(hours=2, minutes=30)
        self.assertEqual(result, expected)

        # Test fractional minutes
        result = base + timedelta(minutes=1.5)
        expected = base + timedelta(minutes=1, seconds=30)
        self.assertEqual(result, expected)

        # Test very small fractions
        result = base + timedelta(microseconds=0.1)
        # Should be effectively zero for TS precision
        self.assertAlmostEqual(float(result), float(base), places=9)

    def test_timedelta_mixed_units_combinations(self):
        """Test complex timedelta combinations with mixed positive/negative units."""
        base = TS("2018-02-28T22:00:00Z")

        # Test mixed signs that cancel out
        td_cancel = timedelta(days=1, hours=-24)
        result = base + td_cancel
        self.assertEqual(result, base)

        # Test complex combination
        td_complex = timedelta(
            weeks=1,
            days=2,
            hours=3,
            minutes=4,
            seconds=5,
            milliseconds=6,
            microseconds=7
        )

        result = base + td_complex
        self.assertIsInstance(result, TS)
        self.assertGreater(result, base)

        # Verify reverse operation
        self.assertEqual(td_complex + base, result)

        # Test that subtraction is inverse of addition
        back_to_base = result - td_complex
        self.assertAlmostEqual(float(back_to_base), float(base), places=9)


class TestTSInterval(TestCase):
    """Comprehensive tests for TSInterval class"""

    def setUp(self):
        """Set up test fixtures"""
        self.ts1 = TS("2018-01-01T00:00:00Z")
        self.ts2 = TS("2018-01-02T00:00:00Z")
        self.ts3 = TS("2018-01-03T00:00:00Z")
        self.ts4 = TS("2018-01-04T00:00:00Z")

        self.interval1 = TSInterval(self.ts1, self.ts2)
        self.interval2 = TSInterval(self.ts2, self.ts3)
        self.interval3 = TSInterval(self.ts1, self.ts3)

    def test_init_valid(self):
        """Test valid interval initialization"""
        interval = TSInterval(self.ts1, self.ts2)
        self.assertIsNotNone(interval)
        self.assertIsInstance(interval.start, BaseTS)
        self.assertIsInstance(interval.end, BaseTS)

    def test_init_with_float(self):
        """Test initialization with float timestamps should raise TypeError"""
        start_float = 1514764800.0  # 2018-01-01T00:00:00Z
        end_float = 1514851200.0    # 2018-01-02T00:00:00Z
        with self.assertRaises(TypeError):
            TSInterval(start_float, end_float)

    def test_init_with_int(self):
        """Test initialization with integer timestamps should raise TypeError"""
        start_int = 1514764800
        end_int = 1514851200
        with self.assertRaises(TypeError):
            TSInterval(start_int, end_int)

    def test_init_invalid_equal_timestamps(self):
        """Test that start == end raises ValueError"""
        with self.assertRaises(ValueError):
            TSInterval(self.ts1, self.ts1)

    def test_init_invalid_start_after_end(self):
        """Test that start > end raises ValueError"""
        with self.assertRaises(ValueError):
            TSInterval(self.ts2, self.ts1)

    def test_properties_start_end(self):
        """Test start and end properties"""
        self.assertEqual(self.interval1.start, self.ts1)
        self.assertEqual(self.interval1.end, self.ts2)

    def test_duration_seconds(self):
        """Test duration property returns dTS in seconds"""
        duration = self.interval1.duration
        self.assertIsInstance(duration, dTS)
        self.assertEqual(str(duration), "1d")  # 1 day in dTS format
        # Verify it represents the correct duration
        self.assertEqual(duration._delta_ns, 86400 * 1_000_000_000)

    def test_duration_ms(self):
        """Test duration_ms property"""
        duration_ms = self.interval1.duration.as_msec()
        self.assertEqual(duration_ms, 86400000)

    def test_duration_us(self):
        """Test duration_us property"""
        duration_us = self.interval1.duration.as_usec()
        self.assertEqual(duration_us, 86400000000)

    def test_duration_ns(self):
        """Test duration_ns property"""
        duration_ns = self.interval1.duration.as_nsec()
        self.assertEqual(duration_ns, 86400000000000)

    def test_midpoint(self):
        """Test midpoint calculation"""
        midpoint = self.interval1.midpoint
        expected = TS((float(self.ts1) + float(self.ts2)) / 2)
        self.assertEqual(midpoint, expected)

    def test_as_iso_format(self):
        """Test ISO 8601 interval representation"""
        iso_str = self.interval1.as_iso
        self.assertIn("/", iso_str)
        parts = iso_str.split("/")
        self.assertEqual(len(parts), 2)
        self.assertEqual(parts[0], self.ts1.as_iso)
        self.assertEqual(parts[1], self.ts2.as_iso)

    def test_as_iso_basic_format(self):
        """Test basic ISO interval representation"""
        basic_str = self.interval1.as_iso_basic
        self.assertIn("/", basic_str)
        parts = basic_str.split("/")
        self.assertEqual(len(parts), 2)
        self.assertEqual(parts[0], self.ts1.as_iso_basic)
        self.assertEqual(parts[1], self.ts2.as_iso_basic)

    def test_contains_inclusive(self):
        """Test contains method with various timestamps"""
        # Start point
        self.assertTrue(self.interval1.contains(self.ts1))
        # End point
        self.assertTrue(self.interval1.contains(self.ts2))
        # Midpoint
        midpoint = TS((float(self.ts1) + float(self.ts2)) / 2)
        self.assertTrue(self.interval1.contains(midpoint))
        # Before interval
        before = TS(float(self.ts1) - 1)
        self.assertFalse(self.interval1.contains(before))
        # After interval
        after = TS(float(self.ts2) + 1)
        self.assertFalse(self.interval1.contains(after))

    def test_contains_with_ts_timestamp(self):
        """Test contains with TS timestamp"""
        mid_ts = TS("2018-01-01T12:00:00Z")
        self.assertTrue(self.interval1.contains(mid_ts))

    def test_contains_exclusive(self):
        """Test contains_exclusive method"""
        # Start point (should exclude)
        self.assertFalse(self.interval1.contains_exclusive(self.ts1))
        # End point (should exclude)
        self.assertFalse(self.interval1.contains_exclusive(self.ts2))
        # Midpoint (should include)
        midpoint = TS((float(self.ts1) + float(self.ts2)) / 2)
        self.assertTrue(self.interval1.contains_exclusive(midpoint))

    def test_overlaps_overlapping(self):
        """Test overlaps with overlapping intervals"""
        # interval1: ts1 to ts2, interval3: ts1 to ts3 (overlaps)
        self.assertTrue(self.interval1.overlaps(self.interval3))
        self.assertTrue(self.interval3.overlaps(self.interval1))

    def test_overlaps_adjacent(self):
        """Test overlaps with adjacent intervals"""
        # interval1: ts1 to ts2, interval2: ts2 to ts3 (touching at ts2, not overlapping)
        self.assertFalse(self.interval1.overlaps(self.interval2))
        self.assertFalse(self.interval2.overlaps(self.interval1))

    def test_overlaps_separated(self):
        """Test overlaps with separated intervals"""
        interval_far = TSInterval(self.ts3, self.ts4)
        self.assertFalse(self.interval1.overlaps(interval_far))

    def test_overlaps_inclusive(self):
        """Test overlaps_inclusive with adjacent intervals"""
        # interval1: ts1 to ts2, interval2: ts2 to ts3 (touching at ts2)
        self.assertTrue(self.interval1.overlaps_inclusive(self.interval2))
        self.assertTrue(self.interval2.overlaps_inclusive(self.interval1))

    def test_overlaps_inclusive_separated(self):
        """Test overlaps_inclusive with separated intervals"""
        interval_far = TSInterval(self.ts3, self.ts4)
        self.assertFalse(self.interval1.overlaps_inclusive(interval_far))

    def test_intersection_overlapping(self):
        """Test intersection of overlapping intervals"""
        # interval1: ts1 to ts2, interval3: ts1 to ts3
        intersection = self.interval1.intersection(self.interval3)
        self.assertIsNotNone(intersection)
        self.assertEqual(intersection.start, self.ts1)
        self.assertEqual(intersection.end, self.ts2)

    def test_intersection_no_overlap(self):
        """Test intersection of non-overlapping intervals"""
        # interval1: ts1 to ts2, interval2: ts2 to ts3 (no overlap)
        intersection = self.interval1.intersection(self.interval2)
        self.assertIsNone(intersection)

    def test_union_overlapping(self):
        """Test union of overlapping intervals"""
        # interval1: ts1 to ts2, interval3: ts1 to ts3
        union = self.interval1.union(self.interval3)
        self.assertIsNotNone(union)
        self.assertEqual(union.start, self.ts1)
        self.assertEqual(union.end, self.ts3)

    def test_union_adjacent(self):
        """Test union of adjacent intervals"""
        # interval1: ts1 to ts2, interval2: ts2 to ts3
        union = self.interval1.union(self.interval2)
        self.assertIsNotNone(union)
        self.assertEqual(union.start, self.ts1)
        self.assertEqual(union.end, self.ts3)

    def test_union_no_overlap(self):
        """Test union of non-touching intervals"""
        interval_far = TSInterval(self.ts3, self.ts4)
        union = self.interval1.union(interval_far)
        self.assertIsNone(union)

    def test_magic_subset_operations(self):
        """Test &, | return intersection and union semantics"""
        interval_a = TSInterval(self.ts1, self.ts3)
        interval_b = TSInterval(self.ts2, self.ts4)
        intersection = interval_a & interval_b
        self.assertEqual(intersection, self.interval2)
        union = interval_a | interval_b
        self.assertEqual(union, TSInterval(self.ts1, self.ts4))
        disjoint = TSInterval(TS("2025-01-01T00:00:00Z"), TS("2025-01-02T00:00:00Z"))
        self.assertIsNone(interval_a & disjoint)
        self.assertIsNone(interval_a | disjoint)

    def test_shift_positive_delta(self):
        """Test shifting interval forward"""
        delta = dTS("1d")
        shifted = self.interval1.shift(delta)
        expected_start = self.ts1 + delta
        expected_end = self.ts2 + delta
        self.assertEqual(shifted.start, expected_start)
        self.assertEqual(shifted.end, expected_end)

    def test_shift_negative_delta(self):
        """Test shifting interval backward"""
        delta = dTS("-1d")
        shifted = self.interval1.shift(delta)
        expected_start = self.ts1 + delta
        expected_end = self.ts2 + delta
        self.assertEqual(shifted.start, expected_start)
        self.assertEqual(shifted.end, expected_end)

    def test_shift_with_float_seconds(self):
        """Test shift with float seconds"""
        shifted = self.interval1.shift(3600.0)  # 1 hour
        self.assertAlmostEqual(float(shifted.start), float(self.ts1) + 3600, places=6)
        self.assertAlmostEqual(float(shifted.end), float(self.ts2) + 3600, places=6)

    def test_shift_with_timedelta(self):
        """Test shift with timedelta"""
        shifted = self.interval1.shift(timedelta(hours=2))
        self.assertAlmostEqual(float(shifted.start), float(self.ts1) + 7200, places=6)
        self.assertAlmostEqual(float(shifted.end), float(self.ts2) + 7200, places=6)

    def test_expand_before(self):
        """Test expanding interval before start"""
        delta = dTS("1d")
        expanded = self.interval1.expand(before=delta)
        expected_start = self.ts1 - delta
        self.assertEqual(expanded.start, expected_start)
        self.assertEqual(expanded.end, self.ts2)

    def test_expand_after(self):
        """Test expanding interval after end"""
        delta = dTS("1d")
        expanded = self.interval1.expand(after=delta)
        self.assertEqual(expanded.start, self.ts1)
        expected_end = self.ts2 + delta
        self.assertEqual(expanded.end, expected_end)

    def test_expand_both(self):
        """Test expanding interval on both ends"""
        before_delta = dTS("6h")
        after_delta = dTS("12h")
        expanded = self.interval1.expand(before=before_delta, after=after_delta)
        self.assertEqual(expanded.start, self.ts1 - before_delta)
        self.assertEqual(expanded.end, self.ts2 + after_delta)

    def test_expand_with_float(self):
        """Test expand with float seconds"""
        expanded = self.interval1.expand(before=3600, after=7200)
        self.assertAlmostEqual(float(expanded.start), float(self.ts1) - 3600, places=6)
        self.assertAlmostEqual(float(expanded.end), float(self.ts2) + 7200, places=6)

    def test_shrink_from_start(self):
        """Test shrinking from start"""
        delta = dTS("6h")
        shrunk = self.interval1.shrink(from_start=delta)
        expected_start = self.ts1 + delta
        self.assertEqual(shrunk.start, expected_start)
        self.assertEqual(shrunk.end, self.ts2)

    def test_shrink_from_end(self):
        """Test shrinking from end"""
        delta = dTS("6h")
        shrunk = self.interval1.shrink(from_end=delta)
        self.assertEqual(shrunk.start, self.ts1)
        expected_end = self.ts2 - delta
        self.assertEqual(shrunk.end, expected_end)

    def test_shrink_from_both(self):
        """Test shrinking from both ends"""
        from_start = dTS("3h")
        from_end = dTS("9h")
        shrunk = self.interval1.shrink(from_start=from_start, from_end=from_end)
        self.assertEqual(shrunk.start, self.ts1 + from_start)
        self.assertEqual(shrunk.end, self.ts2 - from_end)

    def test_shrink_with_float(self):
        """Test shrink with float seconds"""
        shrunk = self.interval1.shrink(from_start=3600, from_end=3600)
        self.assertAlmostEqual(float(shrunk.start), float(self.ts1) + 3600, places=6)
        self.assertAlmostEqual(float(shrunk.end), float(self.ts2) - 3600, places=6)

    def test_shrink_invalid_result(self):
        """Test that shrinking too much raises ValueError"""
        # Shrink by 1.5 days from a 1-day interval
        with self.assertRaises(ValueError):
            self.interval1.shrink(from_start=dTS("1.5d"))

    def test_split_at_start(self):
        """Test splitting at start boundary raises error"""
        # Splitting at the exact start point should raise because it creates an
        # invalid interval with start == end
        with self.assertRaises(ValueError):
            self.interval1.split(self.ts1)

    def test_split_at_midpoint(self):
        """Test splitting at midpoint"""
        midpoint_ts = TS((float(self.ts1) + float(self.ts2)) / 2)
        before, after = self.interval1.split(midpoint_ts)
        self.assertEqual(before.start, self.ts1)
        self.assertEqual(before.end, midpoint_ts)
        self.assertEqual(after.start, midpoint_ts)
        self.assertEqual(after.end, self.ts2)

    def test_split_outside_interval(self):
        """Test splitting outside interval raises ValueError"""
        with self.assertRaises(ValueError):
            self.interval1.split(self.ts3)

    def test_gap_to_separated(self):
        """Test gap calculation for separated intervals"""
        interval_sep = TSInterval(TS(float(self.ts2) + 3600), self.ts3)
        gap = self.interval1.gap_to(interval_sep)
        self.assertEqual(gap, 3600.0)

    def test_gap_to_touching(self):
        """Test gap calculation for touching intervals"""
        gap = self.interval1.gap_to(self.interval2)
        self.assertEqual(gap, 0.0)

    def test_gap_to_overlapping(self):
        """Test gap calculation for overlapping intervals"""
        overlap_interval = TSInterval(
            TS(float(self.ts1) + 43200),  # Half way through interval1
            self.ts3
        )
        gap = self.interval1.gap_to(overlap_interval)
        # Overlap is 43200 seconds (half day)
        self.assertEqual(gap, -43200.0)

    def test_gap_to_reverse(self):
        """Test gap calculation in reverse (other before this)"""
        separated = TSInterval(self.ts1 - 7200, self.ts1 - 3600)
        gap = separated.gap_to(self.interval1)
        self.assertEqual(gap, 3600.0)

    def test_is_before(self):
        """Test is_before method"""
        self.assertTrue(self.interval1.is_before(self.interval2))
        self.assertFalse(self.interval2.is_before(self.interval1))
        self.assertFalse(self.interval1.is_before(self.interval1))

    def test_is_after(self):
        """Test is_after method"""
        self.assertTrue(self.interval2.is_after(self.interval1))
        self.assertFalse(self.interval1.is_after(self.interval2))
        self.assertFalse(self.interval1.is_after(self.interval1))

    def test_is_adjacent_to_true(self):
        """Test is_adjacent_to when intervals are adjacent"""
        self.assertTrue(self.interval1.is_adjacent_to(self.interval2))
        self.assertTrue(self.interval2.is_adjacent_to(self.interval1))

    def test_is_adjacent_to_false(self):
        """Test is_adjacent_to when intervals are not adjacent"""
        far_interval = TSInterval(self.ts3, self.ts4)
        self.assertFalse(self.interval1.is_adjacent_to(far_interval))

    def test_equality_same(self):
        """Test equality with same intervals"""
        interval_dup = TSInterval(self.ts1, self.ts2)
        self.assertEqual(self.interval1, interval_dup)

    def test_equality_different(self):
        """Test inequality with different intervals"""
        self.assertNotEqual(self.interval1, self.interval2)

    def test_equality_with_non_interval(self):
        """Test inequality with non-TSInterval objects"""
        self.assertNotEqual(self.interval1, "not an interval")
        self.assertNotEqual(self.interval1, None)

    def test_less_than(self):
        """Test less than comparison"""
        self.assertTrue(self.interval1 < self.interval2)
        self.assertFalse(self.interval2 < self.interval1)

    def test_less_than_same_start_different_end(self):
        """Test less than with same start, different end"""
        interval_short = TSInterval(self.ts1, TS(float(self.ts2) - 3600))
        self.assertTrue(interval_short < self.interval1)

    def test_ordering_operators(self):
        """Test all ordering operators (from @total_ordering)"""
        self.assertTrue(self.interval1 <= self.interval2)
        self.assertTrue(self.interval2 >= self.interval1)
        self.assertTrue(self.interval2 > self.interval1)
        self.assertFalse(self.interval1 > self.interval2)

    def test_hash(self):
        """Test hash for use in sets and dicts"""
        interval_dup = TSInterval(self.ts1, self.ts2)
        # Same intervals should have same hash
        self.assertEqual(hash(self.interval1), hash(interval_dup))

        # Can be used in sets
        interval_set = {self.interval1, interval_dup, self.interval2}
        self.assertEqual(len(interval_set), 2)

    def test_repr(self):
        """Test string representation"""
        repr_str = repr(self.interval1)
        self.assertIn("TSInterval", repr_str)
        self.assertIn(self.ts1.as_iso, repr_str)
        self.assertIn(self.ts2.as_iso, repr_str)

    def test_str(self):
        """Test string conversion"""
        str_repr = str(self.interval1)
        self.assertEqual(str_repr, self.interval1.as_iso)

    def test_complex_scenario_chain_operations(self):
        """Test complex scenario with chained operations"""
        # Start with interval1
        # Expand it
        expanded = self.interval1.expand(before=dTS("1d"), after=dTS("1d"))
        # Compare the durations in seconds (using _delta_ns)
        self.assertGreater(expanded.duration._delta_ns, self.interval1.duration._delta_ns)

        # Shift it
        shifted = expanded.shift(dTS("2d"))

        # Check that it moved forward by 2 days
        self.assertAlmostEqual(
            float(shifted.start),
            float(expanded.start) + 86400 * 2,
            places=6
        )

    def test_multiple_intervals_operations(self):
        """Test operations with multiple intervals"""
        intervals = [self.interval1, self.interval2]

        # Find overlapping intervals
        test_interval = TSInterval(
            TS(float(self.ts1) + 43200),  # Halfway through interval1
            TS(float(self.ts2) + 43200)   # Halfway through interval2
        )

        overlapping = [i for i in intervals if i.overlaps(test_interval)]
        self.assertEqual(len(overlapping), 2)

    def test_interval_with_very_small_duration(self):
        """Test interval with very small duration"""
        start = TS(1000.0)
        end = TS(1000.001)
        small_interval = TSInterval(start, end)
        # Verify the duration is approximately 0.001 seconds
        self.assertEqual(small_interval.duration.as_msec(), 1)

    def test_interval_with_large_duration(self):
        """Test interval with very large duration (years)"""
        start = TS("2000-01-01T00:00:00Z")
        end = TS("2020-01-01T00:00:00Z")
        large_interval = TSInterval(start, end)
        # 20 years = approx 7305 days worth of nanoseconds
        expected_ns = 7000 * 86400 * 1_000_000_000
        self.assertGreater(large_interval.duration.as_nsec(), expected_ns)


class TestDTS(TestCase):
    def test_conversion_helpers_return_ints(self):
        delta = dTS("1500000ns")
        self.assertEqual(delta.as_nsec(), 1_500_000)
        self.assertEqual(delta.as_usec(), 1_500)
        self.assertEqual(delta.as_msec(), 2)
        self.assertEqual(delta.as_sec(), 0)

    def test_rounding_behavior(self):
        delta = dTS("900000ns")
        self.assertEqual(delta.as_nsec(), 900_000)
        self.assertEqual(delta.as_usec(), 900)
        self.assertEqual(delta.as_msec(), 1)
        self.assertEqual(delta.as_sec(), 0)

    def test_second_rounding_up(self):
        delta = dTS("1500000000ns")
        self.assertEqual(delta.as_nsec(), 1_500_000_000)
        self.assertEqual(delta.as_usec(), 1_500_000)
        self.assertEqual(delta.as_msec(), 1_500)
        self.assertEqual(delta.as_sec(), 2)


if __name__ == "__main__":
    unittest.main()
