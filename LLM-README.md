## API Quick‑Reference (tsx) to be used by LLMs

Use this section as a compact, lossless summary of the public API and semantics.

- Core types
  - TS(float): Unix seconds since Epoch, subclass of float
  - TSMsec(float): TS factory with default prec="ms" during construction
  - iTS(int): integer seconds; iTSms(int): integer milliseconds; iTSus(int): integer microseconds; iTSns(int): integer nanoseconds. All subclass int and BaseTS
  - All classes share BaseTS helpers (formatting, tz conversions, floor/ceil)

- Construction
  - TS(ts, prec="s", utc=True) where ts ∈ {int|float|str|datetime|date|TS}
    - Strings: ISO‑8601. If string has TZ info, utc arg is ignored. If no TZ: utc=True → treat as UTC; utc=False → treat as local time
    - prec indicates input numeric unit: "s"|"ms"|"us"|"ns"
  - TSMsec(ts, prec="ms") same as TS but default prec="ms"
  - iTS/iTSms/iTSus/iTSns accept numbers/strings/TS/other iTS* and convert to their precision. Their .timestamp() always returns TS

- Now/Time
  - TS.now() → TS; iTS*.now() → respective integer class using time_ns()
  - TS.now_dt() → datetime(tz=UTC)

- Formatting and conversion
  - x.isoformat(sep="T", timespec) → ISO string; TS supports standard timespec; iTSns defaults to nanoseconds with trailing Z
  - x.as_iso, x.as_iso_date, x.as_iso_date_basic, x.as_iso_tz(tzinfo|"Area/City"), x.as_iso_basic
  - x.as_sec()->iTS; x.as_msec()->iTSms; x.as_usec()->iTSus; x.as_nsec()->iTSns (TS only). Deprecated properties exist for backward compat: TS.as_ms, TS.as_sec
  - x.as_dt(tz=UTC) returns aware datetime; x.as_local_dt() returns aware local datetime

- Arithmetic (return types preserve class of left operand)
  - +/‑ with numbers:
    - TS: number interpreted in seconds
    - iTS*: number interpreted in that class’s unit (sec, ms, us, ns)
  - +/‑ with datetime.timedelta (supported for TS and all iTS*):
    - TS: adds td.total_seconds() (float)
    - iTS*: adds round(td.total_seconds()*UNITS_IN_SEC). Rounding uses Python round() (banker’s rounding)
  - +/‑ with dTS (calendar delta): months/years/weeks/days/hours/minutes/seconds/ms/us/ns; months/years applied via relativedelta while keeping time‑of‑day remainder; computed internally at ns granularity
  - TS‑TS and iTS*‑iTS* produce same class with appropriate difference semantics

- Floor/Ceil
  - TS.floor(unit_s: float)->TS, TS.ceil(unit_s: float)->TS; unit must be multiple of 1ms (checked via rounding to ms)
  - iTS*.floor(unit)->same class; unit in the class’s integral units

- Parsing
  - TS.from_iso(s, utc=True) like constructor but explicit. BaseTS.ns_timestamp_from_iso(s, utc) handles 7‑9 fractional digits; iTSns also parses basic/extended ISO with 7‑9 decimals
  - Python ≥3.11 uses datetime.fromisoformat; older uses ciso8601 with python‑dateutil fallback

- Timezones
  - If input has TZ offset/Z → used as provided. If no TZ and utc=True → assume UTC; if utc=False → local timezone

- Pydantic
  - Pydantic v1: __get_validators__; v2: __get_pydantic_core_schema__ using pydantic‑core

- Constants (exported)
  - FIRST_MONDAY_TS, DAY_SEC, DAY_MSEC, WEEK_SEC

- Edge/Notes
  - TS.as_dt handles very large years (>2038) without overflow
  - timedelta has microsecond resolution; iTSns arithmetic with timedelta is limited by that resolution and float precision
  - iTSns.isoformat defaults to nanoseconds and appends Z