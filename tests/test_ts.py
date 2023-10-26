#!/usr/bin/env python
# coding:utf-8
# Author: ASU --<andrei.suiu@gmail.com>
# Purpose:
# Created: 8/4/2021

__author__ = "ASU"

import unittest
from datetime import datetime, timezone
from time import time, localtime, strftime
from unittest import TestCase

import numpy as np
import pytz
from dateutil import tz

from tsx import TS, TSMsec


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

    def test_to_int(self):
        ts = TS(ts=self.INT_BASE_ROUND_MS_TS + 500, prec="ms")
        self.assertEqual(int(ts), self.INT_BASE_TS)
        # round up
        ts = TS(ts=self.INT_BASE_ROUND_MS_TS + 501, prec="ms")
        self.assertEqual(int(ts), self.INT_BASE_TS + 1)

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
        self.assertEqual(ts.as_ms, 1519855200123)
        # round up
        ts = TS(ts=1519855200.123856)
        self.assertEqual(ts.as_ms, 1519855200124)

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

    def test_math_ops(self):
        ts = TS(ts=1519855200)
        self.assertEqual((ts + 20).as_ms, 1519855220000)
        self.assertEqual(TS(20 + ts).as_ms, 1519855220000)
        self.assertEqual((ts - 0.5).as_ms, 1519855199500)

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


if __name__ == "__main__":
    unittest.main()
