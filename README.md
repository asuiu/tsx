# tsx
Time Stamp eXtensions for Python

### Usage:

TS(ts: Union[int, float, str], prec: Literal["s", "ms"] = "s")
TSMsec(ts: Union[int, float, str], prec: Literal["s", "ms"] = "ms")

prec - means precision of the `ts` argument.
    If prec=="s" - the `ts` argument will be interpreted as nr of seconds since epoch,
    If prec=="s" - the `ts` argument will be interpreted as nr of milliseconds since epoch 

Example:
ts = TS(ts="1519855200.123856", prec="s")

ts==1519855200.123856
ts.as_iso == '2018-02-28T22:00:00.123856Z'
ts.as_iso_tz(pytz.timezone("Europe/Bucharest"))=='2018-03-01T00:00:00.123856+02:00'

