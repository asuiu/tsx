# tsx

**T**ime **S**tamp e**X**tensions for Python

This library was created as a response to the known Python datetime standard library flaw that violates ISO
8601. ( [Example](https://stackoverflow.com/questions/19654578/python-utc-datetime-objects-iso-format-doesnt-include-z-zulu-or-zero-offset) )

Under the hood, it uses external dateparser library that's fully compatible with ISO 8601, and it simplifies working
with date & time stamps.

It also handles properly the Daylight Saving Time (summer time).

### Installation

`pip pinstall tsx`

### Usage:

The library is pretty simple, its central class is `TS`, which inhertis Python builtin `float`,
so every timestamp in fact is a float representing number of seconds since Epoch.

The `TSMsec` is the same `TS` with only difference that it's constructor by default expects msec precision, i.e. number
of msecs since epoch,
but internally it will store the same float as number of seconds since Epoch.

```python
TS(ts: Union[int, float, str], prec: Literal["s", "ms"] = "s")`

TSMsec(ts: Union[int, float, str], prec: Literal["s", "ms"] = "ms")
```

- `prec` - is precision of the `ts` argument.
    - If `prec=="s"` - the `ts` argument will be interpreted as nr of seconds since epoch,
    - If `prec=="ms"` - the `ts` argument will be interpreted as nr of milliseconds since epoch

### Example:

```python
ts = TS(ts="1519855200.123856", prec="s")

ts == 1519855200.123856
ts.as_iso == '2018-02-28T22:00:00.123856Z'
ts.as_iso_tz(pytz.timezone("Europe/Bucharest")) == '2018-03-01T00:00:00.123856+02:00'

TS("2018-02-28T22:00:00.123Z")
TS("2018-02-28T22:00:00.123")

TS("2018-02-28T22:00:00.123+00:00")
```

```python
ts = TS.now()

ts.as_sec == 1234567890.123
ts.as_ms == 1234567890123
ts.as_file_date == '20090213'
ts.as_file_ts == '20090213-233130'
```

### Changelog

##### 0.1.0

- Added the `utc:bool=True` parameter to TS constructor, which if set to `True` (by default) will force the timestamp to
  be interpreted as UTC, thus `TS('2018-02-28T22:00:00') will be interpreted as UTC, and not as local time, even if it
  doesn't have explicit TZ info.
- Improved speed of TS.from_iso(). For Python <3.11 it uses [`ciso8601`](https://github.com/closeio/ciso8601) which is
  the fastest ISO 8601 parser, and for Python >= 3.11 it uses the builtin `datetime.fromisoformat()`.
- some minor parsing speed improvements
- added public time utility variables `FIRST_MONDAY_TS`, `DAY_SEC`, `DAY_MSEC`, `WEEK_SEC`