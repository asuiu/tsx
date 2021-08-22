# tsx
**T**ime **S**tamp e**X**tensions for Python

This library was created as a response to the known Python datetime standard library flaw that violates ISO 8601. ( [Example](https://stackoverflow.com/questions/19654578/python-utc-datetime-objects-iso-format-doesnt-include-z-zulu-or-zero-offset) )

Under the hood, it uses external dateparser library that's fully compatible with ISO 8601, and it simplifies working with date & time stamps.

It also handles properly the Daylight Saving Time (summer time).  

### Usage:
The library is pretty simple, its central class is `TS`, which inhertis Python builtin `float`, 
so every timestamp in fact is a float representing number of seconds since Epoch.

The `TSMsec` is the same `TS` with only difference that it's constructor by default expects msec precision, i.e. number of msecs since epoch,
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

ts==1519855200.123856
ts.as_iso == '2018-02-28T22:00:00.123856Z'
ts.as_iso_tz(pytz.timezone("Europe/Bucharest"))=='2018-03-01T00:00:00.123856+02:00'

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