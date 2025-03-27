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
from datetime import datetime, timezone, date
from time import time, localtime, strftime, time_ns
from unittest import TestCase
from unittest.mock import patch

import numpy as np
import pytz
from dateutil import tz
from pydantic import BaseModel

from tsx import TS, TSMsec, iTS, iTSms, iTSus, iTSns
from tsx.ts import dTS


class TestTS(TestCase):
    INT_BASE_TS = 1519855200
    INT_BASE_ROUND_MS_TS = 1519855200000
    INT_BASE_MS_TS = 1519855200123
    FLOAT_MS_TS = 1519855200.123856
    STR_SEC_TS = "2018-02-28T22:00:00Z"
    AS_FILE_SEC_TS = "20180228-220000"
    STR_MSEC_TS = "2018-02-28T22:00:00.123000Z"

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
        self.assertEqual(ts.as_iso, self.STR_MSEC_TS)

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
        self.assertEqual(ts.as_dt(), datetime(9999, 12, 31))

        ts = TS('9999-12-31T23:59:59')
        self.assertEqual(ts.as_dt(), datetime(9999, 12, 31, 23, 59, 59))

    def test_str_infinite_dates(self):
        ts = TS('9999-12-31')
        self.assertEqual(str(ts), '9999-12-31T00:00:00')

        ts = TS('9999-12-31T23:59:59')
        self.assertEqual(str(ts), '9999-12-31T23:59:59')

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
        ts = TS.from_iso("2018-02-28")
        self.assertEqual(ts, TS("2018-02-28T00:00:00Z"))
        ts = TS.from_iso("2018-02-28", utc=False)
        self.assertEqual(ts, TS("2018-02-28T00:00:00", utc=False))

        ts = TS.from_iso("20180228")
        self.assertEqual(ts, TS("2018-02-28T00:00:00Z"))

        ts = TS.from_iso("2018-02-28T22:00:00+00:00")
        self.assertEqual(ts, TS("2018-02-28T22:00:00Z"))

        ts = TS.from_iso("2018-02-28T00:00:00+02:00")
        self.assertEqual(ts, TS("2018-02-27T22:00:00Z"))
        # ts = TS.from_iso("2018")
        # self.assertEqual(ts, TS("2018-01-01T00:00:00Z"))
        if sys.version_info <= (3, 11):
            ts = TS.from_iso("2018-02")
            self.assertEqual(ts, TS("2018-02-01T00:00:00Z"))

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
        ts = TS.from_iso("2022-12-07T00:00:01")
        self.assertEqual(ts.weekday(), 2)
        ts = TS.from_iso("2022-12-07T00:00:00+02", utc=False)
        self.assertEqual(ts.weekday(utc=False), 2)
        self.assertEqual(ts.weekday(), 1)

    def test_isoweekday(self):
        ts = TS.from_iso("2022-12-07T00:00:01")
        self.assertEqual(ts.isoweekday(), 3)
        ts = TS.from_iso("2022-12-07T00:00:00+02", utc=False)
        self.assertEqual(ts.isoweekday(utc=False), 3)
        self.assertEqual(ts.isoweekday(), 2)

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
        self.assertEqual(nsec, 1670371200123456768)

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
    def test_timestamp(self):
        ts = TS(1519855200.123)
        self.assertIsInstance(ts.timestamp(), float)
        self.assertEqual(ts.timestamp(), 1519855200.123)


class Test_iTS(TestCase):
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
        self.assertEqual(res, "2018-03-01T02:00:00+02:00")
        res = ts.iso_tz("Europe/Bucharest")
        self.assertEqual(res, "2018-03-01T02:00:00+02:00")

    def test_as_iso_tz_DST(self):
        ts = iTS("2020-06-01T10:00:00Z")
        res = ts.iso_tz(pytz.timezone("Europe/Bucharest"))
        self.assertEqual(res, "2020-06-01T13:00:00+03:00")

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
        self.assertEqual(repr(ts), "iTSms('2018-02-28T22:00:00.123000Z')")

    def test_str(self):
        ts = iTSms(ts=TestTS.INT_BASE_MS_TS)
        self.assertEqual(str(ts), TestTS.STR_MSEC_TS)

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
    def test_constructors(self):
        ts = TS(1519855200.123456789)
        its = iTSns(ts)
        self.assertIsInstance(its, iTSns)
        self.assertEqual(its, 1519855200123456768)

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
        ts_ms_gran = int(TS("2022-12-07T10:00:00.123456", utc=False).as_msec())*1_000_000
        its_us_gran = int(iTSns("2022-12-07T10:00:00.123456", utc=False))
        its_ns_gran = int(iTSns("2022-12-07T10:00:00.123456789", utc=False))
        self.assertEqual(ts_ms_gran+456000, its_us_gran)
        self.assertEqual(its_us_gran+789, its_ns_gran)

    def test_constructor_with_tz_no_utc(self):
        ts_ms_gran = int(TS("2022-12-07T10:00:00.123456+04", utc=False).as_msec())*1_000_000
        its_us_gran = int(iTSns("2022-12-07T10:00:00.123456+04", utc=False))
        its_ns_gran = int(iTSns("2022-12-07T10:00:00.123456789+04", utc=False))
        self.assertEqual(ts_ms_gran+456000, its_us_gran)
        self.assertEqual(its_us_gran+789, its_ns_gran)

    def test_constructor_with_utc_and_tz(self):
        ts_ms_gran = int(TS("2022-12-07T10:00:00.123456+04", utc=True).as_msec())*1_000_000
        its_us_gran = int(iTSns("2022-12-07T10:00:00.123456+04", utc=True))
        its_ns_gran = int(iTSns("2022-12-07T10:00:00.123456789+04", utc=True))
        self.assertEqual(ts_ms_gran+456000, its_us_gran)
        self.assertEqual(its_us_gran+789, its_ns_gran)

    def test_constructor_with_utc_no_tz(self):
        ts_ms_gran = int(TS("2022-12-07T10:00:00.123456Z", utc=True).as_msec())*1_000_000
        its_us_gran = int(iTSns("2022-12-07T10:00:00.123456Z", utc=True))
        its_ns_gran = int(iTSns("2022-12-07T10:00:00.123456789Z", utc=True))
        self.assertEqual(ts_ms_gran+456000, its_us_gran)
        self.assertEqual(its_us_gran+789, its_ns_gran)

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

        self.assertLess(ns_ts - now_ts, 100)
        self.assertLess(abs(its - now_ts), 500)

    def test_pickle_roundtrip(self):
        ts = iTSns("2022-12-07T00:00:00.123456Z")
        ts2 = pickle.loads(pickle.dumps(ts))
        self.assertEqual(ts, ts2)


if __name__ == "__main__":
    unittest.main()
