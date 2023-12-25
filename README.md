# tsx

**T**ime **S**tamp e**X**tensions for Python

###### Why tsx? 
**tsx** was created as a response to the known Python datetime standard library flaw that violates ISO 8601. ( [Example](https://stackoverflow.com/questions/19654578/python-utc-datetime-objects-iso-format-doesnt-include-z-zulu-or-zero-offset) )

It properly handles the Daylight Saving Time (summer time), and provides functionality for creating, manipulating, and formatting timestamps in various formats
and precisions.

Under the hood, it uses external dateparser library that's fully compatible with ISO 8601, and it simplifies working with date & time stamps.

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

## Classes

### `TS`

The TS class, a subclass of float, represents Unix timestamps in seconds. It includes additional methods for timestamp manipulation and formatting.

#### Key Methods and Properties

- `now_dt()`: Returns the current datetime in UTC.
- `now_ms()`, now_us, now_ns: Returns the current timestamp in various precisions.
- `now()`: Returns the current TS instance.
- `from_iso()`: Parses an ISO string to a TS instance.
- `timestamp()`: Returns the timestamp as a TS instance.
- `as_iso()`, `as_iso_date()`, `as_iso_date_basic()`, `as_iso_tz()`, `as_iso_basic()`: Various ISO format representations.
- `as_file_ts()` and `as_file_date()`: File-friendly timestamp formats.
- `as_sec()`, `as_ms()`, `to_sec()`: Conversions to different precisions with deprecation notices.
- `floor()` and `ceil()`: Methods for flooring and ceiling the timestamp.
- `weekday()` and `isoweekday()`: Methods to get the day of the week.
- *Arithmetic Operations*: Overloaded methods for arithmetic.

#### now_dt

- **Description**: Returns the current datetime in UTC.
- **Example**:
  ```python
  current_dt = TS.now_dt()
  current_dt == datetime.datetime(2021, 10, 15, 12, 0, 0, 123456, tzinfo=datetime.timezone.utc)
  ```

#### now

- **Description**: Returns the current timestamp in seconds.
- **Example**:
  ```python
  current_ts = TS.now()
  current_ts == TS(1634294400.123456) == 1634294400.123456
  ```

#### now_ms

- **Description**: Returns the current timestamp in milliseconds.
- **Example**:
  ```python
  current_ts = TS.now_ms()
  current_ts == iTSms(1634294400123) == 1634294400123
  ```

#### now_us

- **Description**: Returns the current timestamp in microseconds.
- **Example**:
  ```python
  current_ts = TS.now_us()
  current_ts == iTSus(1634294400123456) == 1634294400123456
  ```

#### now_ns

- **Description**: Returns the current timestamp in nanoseconds.
- **Example**:
  ```python
  current_ts = TS.now_ns()
  current_ts == iTSn(s1634294400123456789) == 1634294400123456789
  ```

#### from_iso

- **Description**: Parses an ISO string to a TS instance.
- **Example**:
  ```python
  ts = TS.from_iso("2021-10-15T12:00:00.123456Z")
  ts == TS(1634294400.123456) == 1634294400.123456
  ```

#### timestamp

- **Description**: Returns the timestamp as a TS instance.
- **Example**:
  ```python
  ts = TS.timestamp(1634294400.123456)
  ts == TS(1634294400.123456) == 1634294400.123456
  ```

#### as_iso

- **Description**: Returns the timestamp as an ISO string.
- **Example**:
  ```python
  ts = TS(1634294400.123456)
  ts.as_iso == '2021-10-15T12:00:00.123456Z'
  ```

#### as_iso_date

- **Description**: Returns the timestamp as an ISO date string.
- **Example**:
  ```python
  ts = TS(1634294400.123456)
  ts.as_iso_date == '2021-10-15'
  ```

#### as_iso_date_basic

- **Description**: Returns the timestamp as an ISO date string in basic format.
- **Example**:
  ```python
  ts = TS(1634294400.123456)
  ts.as_iso_date_basic == '20211015'
  ```

#### as_iso_tz

- **Description**: Returns the timestamp as an ISO string with timezone.
    - **Parameters**:
        - `tz: str|tzinfo`: The timezone to use.
    - **Example**:
      ```python
      ts = TS(1634294400.123456)
      ts.as_iso_tz(pytz.timezone("Europe/Bucharest")) == '2021-10-15T14:00:00.123456+02:00'
      ```

#### as_iso_basic

- **Description**: Returns the timestamp as an ISO string in basic format.
- **Example**:
  ```python
  ts = TS(1634294400.123456)
  ts.as_iso_basic == '20211015T120000.123456Z'
  ```

#### as_file_ts

- **Description**: Returns the timestamp as a file-friendly timestamp string.
- **Example**:
  ```python
  ts = TS(1634294400.123456)
  ts.as_file_ts == '20211015-120000'
  ```

#### as_file_date

- **Description**: Returns the timestamp as a file-friendly date string.
- **Example**:
  ```python
  ts = TS(1634294400.123456)
  ts.as_file_date == '20211015'
  ```

#### as_sec

- **Description**: Returns the timestamp as a TS instance in seconds.
- **Example**:
  ```python
  ts = TS(1634294400.123456)
  ts.as_sec == TS(1634294400.0) == 1634294400.0
  ```

#### as_ms

- **Description**: Returns the timestamp as a TS instance in milliseconds.
- **Example**:
  ```python
  ts = TS(1634294400.123456)
  ts.as_ms == iTSms(1634294400123) == 1634294400123
  ```

#### to_sec

- **Description**: Returns the timestamp as a TS instance in seconds.
- **Example**:
  ```python
  ts = TS(1634294400.123456)
  ts.to_sec == TS(1634294400.0) == 1634294400.0
  ```

#### floor

- **Description**: Floors the timestamp to the nearest second.
    - **Parameters**:
        - `unit: int|float`: the unit to ceil which should be of the same precision as the timestamp
- **Example**:
  ```python
  ts = TS(1634294413.123456)
  ts.floor(100) == TS(1634294400.0) == 1634294400.0
  
  ts.floor(0.025) == TS(1634294413.1) == 1634294400.1
  ```

#### ceil

- **Description**: Ceils the timestamp to the nearest second.
    - **Parameters**:
        - `unit: int|float`: the unit to ceil which should be of the same precision as the timestamp
    - **Example**:
      ```python
      ts = TS(1634294413.123456)
      ts.ceil(100) == TS(1634294500.0) == 1634294500.0
      
      ts.ceil(0.025) == TS(1634294413.125) == 1634294500.125
      ```

#### weekday

- **Description**: Return the day of the week as an integer, where Monday is 0 and Sunday is 6. See also isoweekday().
    - **Parameters**:
        - `utc: bool = True`: Whether to use UTC or local time.
    - **Example**:
      ```python
      ts = TS(1634294400.123456)
      ts.weekday() == 4
      ```

#### isoweekday

- **Description**: Return the day of the week as an integer, where Monday is 1 and Sunday is 7. See also weekday().
    - **Parameters**:
        - `utc: bool = True`: Whether to use UTC or local time.
    - **Example**:
      ```python
      ts = TS(1634294400.123456)
      ts.isoweekday() == 5
      ```

#### Arithmetic Operations

- **Description**: Overloaded methods for arithmetic.
    - **Parameters**:
        - `other: Union[TS, int, float]`: The other timestamp to use.
    - **Example**:
      ```python
      ts = TS(1634294400.123456)
      ts + 100 == TS(1634294500.123456) == 1634294500.123456
      ts - 100 == TS(1634294300.123456) == 1634294300.123456
      ts * 100 == TS(163429440012.3456) == 163429440012.3456
      ts / 100 == TS(16342944.00123456) == 16342944.00123456
      ts // 100 == TS(16342944.0) == 16342944.0
      ts % 100 == TS(1634294400.123456) == 1634294400.123456
      ts ** 100 == TS(1634294400.123456) == 1634294400.123456
      ```

### `TSMsec`

The TSMsec class, a subclass of float, and it's used as a factory class to instantiate TS from milliseconds precision.

After instantiation, the TSMsec instance is identical to TS instance, and it includes all the same methods and properties.

### `iTS`

- The iTS class, a subclass of int, represents Unix timestamps in seconds. It includes additional methods for timestamp manipulation and formatting.
- It inherits from `BaseTS` and `int` classes, so it exposes all the methods `TS` has, as well as it supports all the arithmetic operations `int` supports.
- It's identical to `TS` class, but all the methods that return `TS` will return `iTS` instead, excepting the timestamp(), which returns `TS`.

#### Key Methods and Properties

- The same as `TS` class, but all the methods that return `TS` will return `iTS` instead.

### `iTSms`

- The iTSms class, a subclass of int, represents Unix timestamps in milliseconds. It includes additional methods for timestamp manipulation and formatting.
- It inherits from `BaseTS` and `int` classes, so it exposes all the methods `TS` has, as well as it supports all the arithmetic operations `int` supports.
- It's identical to `TSMsec` class, but all the methods that return `TS` will return `iTSms` instead, excepting the timestamp(), which returns `TS`.

### `iTSus`

- The iTSus class, a subclass of int, represents Unix timestamps in microseconds. It includes additional methods for timestamp manipulation and formatting.
- It inherits from `BaseTS` and `int` classes, so it exposes all the methods `TS` has, as well as it supports all the arithmetic operations `int` supports.
- It's identical to `TS` class, but all the methods that are expected to return `TS` will return `iTSus` instead, excepting the timestamp(), which returns `TS`.

### `iTSns`

- The iTSns class, a subclass of int, represents Unix timestamps in nanoseconds. It includes additional methods for timestamp manipulation and formatting.
- It inherits from `BaseTS` and `int` classes, so it exposes all the methods `TS` has, as well as it supports all the arithmetic operations `int` supports.
- It's identical to `TS` class, but all the methods that are expected to return `TS` will return `iTSns` instead, excepting the timestamp(), which returns `TS`.
- **Note**: The `iTSns` class is only available for Python >= 3.8, and it support ns level now precision by using `time.time_ns()` instead of `time.time()`.

### Changelog

##### 0.1.16
- Fixed bug in iTS().__sub__ when performed operations with floats or other TS instances
##### 0.1.15
- TS.as_dt() now is able to properly handle the big dates (year > 2038), which are causing overflow exceptions in Python datetime.fromtimestamp() stdlib functions
- Added instantiation from Python datetime and date objects + proper handling of big dates (year > 2038) 

##### 0.1.14
- TS.now() offers nanosecond precision instead of millisecond

##### 0.1.13
- TypeHint update: `TS.as_ms()` now returns `iTSms` instead of simple `int`
- Added more documentation to README.md

##### 0.1.12

- Added dTS object

##### 0.1.11

- fixed the pickling/unpickling of TSMsec objects by instantiating the TSMsec as actually an instance of TS

##### 0.1.10

- upgrade dependency ciso8601 2.3.0 -> 2.3.1

##### 0.1.9

- Fixed bug in TS.__sub__ and TS.__add__ introduced in 0.1.7

##### 0.1.8

- Added TS.to_sec as temporary alias for TS.as_sec

##### 0.1.7

- Added iTS, iTSms, iTSus, iTSns classes.
- deprecated TS.as_msec and TS.as_sec
- No breaking changes yet

##### 0.1.6

- Fixed bug in TSMsec from TSMsec initialization

##### 0.1.5

- Fixed bug in parsing with date_util the Truncated formats with no TZ info

##### 0.1.4

- Exporting FIRST_MONDAY_TS, DAY_SEC, DAY_MSEC, WEEK_SEC into tsx public space

##### 0.1.3

- Fixed bug in TSMsec(<ISO_STRING>)

##### 0.1.2

- Added as_dt() and as_local_dt() methods

##### 0.1.1

- fixed bug in converting from numpy numbers

##### 0.1.0

- Added the `utc:bool=True` parameter to TS constructor, which if set to `True` (by default) will force the timestamp to
  be interpreted as UTC, thus `TS('2018-02-28T22:00:00') will be interpreted as UTC, and not as local time, even if it
  doesn't have explicit TZ info.
- Improved speed of TS.from_iso(). For Python <3.11 it uses [`ciso8601`](https://github.com/closeio/ciso8601) which is
  the fastest ISO 8601 parser, and for Python >= 3.11 it uses the builtin `datetime.fromisoformat()`.
- some minor parsing speed improvements
- added public time utility variables `FIRST_MONDAY_TS`, `DAY_SEC`, `DAY_MSEC`, `WEEK_SEC`

##### 0.0.9

- str(ts) now returns ts.as_iso

##### 0.0.8

- added weekday() + isoweekday()

##### 0.0.7

- added floor() and ceil() methods

##### 0.0.6

- added TS.as_iso_date_basic and as_iso_basic

##### 0.0.5

- added TS.from_iso()

##### 0.0.4

- added return typehint to TS.now()

##### 0.0.3

- Lower the minimal typing-extensions version