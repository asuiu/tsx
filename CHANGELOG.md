# Changelog

## 0.3.4

- Made `dTS` immutable, hashable, and pickle-compatible, with construction performed in `__new__`.
- Added `timedelta` and component-based construction, including integer `months` and `years` and mixed calendar/fixed durations.
- Added native `dTS` addition and subtraction for all timestamp precisions, preserving concrete timestamp types and enabling `TSInterval` arithmetic with integer endpoints.

## 0.3.3

- Aligned arithmetic and comparison type hints across `BaseTS` and the integer timestamp hierarchy.
- Added positional-only operator operands and removed redundant numeric type checks.

## 0.3.2

- Standardized `as_sec()` as a method across timestamp classes, returning `iTS` for `TS` values.
- Updated tests and documentation for the method-based API.

## 0.3.1

- Renamed mock time providers to the `iTS*ProviderMock` naming scheme.
- Aligned mock providers with the production provider inheritance hierarchy and expanded mock-provider tests.

## 0.3.0

- Added real and mock time-provider classes for second, millisecond, microsecond, and nanosecond timestamps.
- Added controllable mock clocks with sleep, advance, and scheduled-callback support.

## 0.2.18

- Corrected timezone conversion in `as_dt()` and `iso_tz()` so timestamps are converted from UTC rather than localized as wall-clock values.
- Added precision-preserving `iso_basic()` implementations for `iTSms` and `iTSus`.
- Updated the supported Python versions to 3.10 and newer.

## 0.2.17

- Added `BaseTS.from_parts()` and `TSInterval.from_year()`.
- Added timezone-name support to `from_parts()`, `as_dt()`, and `iso_tz()` APIs.

## 0.2.16

- Fixed `TSInterval.__repr__()` and `TSInterval.__str__()` formatting.

## 0.2.15

- Added immutable `TSInterval` values with validation, interval properties, transformations, intersections, unions, overlap checks, adjacency checks, and gap calculations.

## 0.2.14

- Fixed `iTSns.iso_basic()` and parsing of basic ISO 8601 nanosecond timestamps.

## 0.2.13

- Fixed `iTSns.from_iso()` parsing.

## 0.2.12

- Improved ISO timestamp parsing errors and added regression coverage for `iTSms` and `iTSns` error messages.

## 0.2.11

- Fixed `iTSns` parsing of millisecond-precision ISO timestamps.

## 0.2.10

- Added support for year-only timestamps such as `2018` and `2018Z`.
- Added pre-commit configuration and improved cross-platform test compatibility.

## 0.2.9

- Added configurable separators and the `use_zulu` option to `iso_date()`, `iso_date_basic()`, and `iso_basic()` formatting.

## 0.2.8

- Added ordering support across timestamp classes.
- Fixed integer timestamp constructors to preserve the requested concrete precision class.

## 0.2.7

- Added precision-aware equality and hashing so equivalent `BaseTS` values are interchangeable as mapping keys.

## 0.2.6

- Added `TS.from_parts_utc()` for constructing timestamps from calendar and subsecond components.

## 0.2.5

- Improved type hints for integer timestamp classes.

## 0.2.4

- Added `datetime.timedelta` arithmetic for `TS`, `iTS`, `iTSms`, `iTSus`, and `iTSns`, with precision-aware rounding for integer classes.
- Added comprehensive arithmetic tests and the `LLM-README.md` API quick reference.

## 0.2.3

- Removed the NumPy warning emitted when parsing Z-suffixed nanosecond timestamp strings.

## 0.2.2

- Made imports resilient when Pydantic is not installed.

## 0.2.1

- Added Pydantic v2 support.
- Made the `typing_extensions` import conditional for compatible Python versions.

## 0.2.0

- The iTSns now has real nanosecond precision (no roundings/approximations) by using the numpy.datetime64 class
- Added the dependency on numpy >=1.8.0 for ns precision

## 0.1.16

- Fixed bug in iTS().__sub__ when performed operations with floats or other TS instances

## 0.1.15

- TS.as_dt() now is able to properly handle the big dates (year > 2038), which are causing overflow exceptions in Python datetime.fromtimestamp() stdlib functions
- Added instantiation from Python datetime and date objects + proper handling of big dates (year > 2038)

## 0.1.14

- TS.now() offers nanosecond precision instead of millisecond

## 0.1.13

- TypeHint update: `TS.as_ms()` now returns `iTSms` instead of simple `int`
- Added more documentation to README.md

## 0.1.12

- Added dTS object

## 0.1.11

- fixed the pickling/unpickling of TSMsec objects by instantiating the TSMsec as actually an instance of TS

## 0.1.10

- upgrade dependency ciso8601 2.3.0 -> 2.3.1

## 0.1.9

- Fixed bug in TS.__sub__ and TS.__add__ introduced in 0.1.7

## 0.1.8

- Added TS.to_sec as temporary alias for TS.as_sec

## 0.1.7

- Added iTS, iTSms, iTSus, iTSns classes.
- deprecated TS.as_msec and TS.as_sec
- No breaking changes yet

## 0.1.6

- Fixed bug in TSMsec from TSMsec initialization

## 0.1.5

- Fixed bug in parsing with date_util the Truncated formats with no TZ info

## 0.1.4

- Exporting FIRST_MONDAY_TS, DAY_SEC, DAY_MSEC, WEEK_SEC into tsx public space

## 0.1.3

- Fixed bug in TSMsec(<ISO_STRING>)

## 0.1.2

- Added as_dt() and as_local_dt() methods

## 0.1.1

- fixed bug in converting from numpy numbers

## 0.1.0

- Added the `utc:bool=True` parameter to TS constructor, which if set to `True` (by default) will force the timestamp to
  be interpreted as UTC, thus `TS('2018-02-28T22:00:00') will be interpreted as UTC, and not as local time, even if it
  doesn't have explicit TZ info.
- Improved speed of TS.from_iso(). For Python <3.11 it uses [`ciso8601`](https://github.com/closeio/ciso8601) which is
  the fastest ISO 8601 parser, and for Python >= 3.11 it uses the builtin `datetime.fromisoformat()`.
- some minor parsing speed improvements
- added public time utility variables `FIRST_MONDAY_TS`, `DAY_SEC`, `DAY_MSEC`, `WEEK_SEC`

## 0.0.9

- str(ts) now returns ts.as_iso

## 0.0.8

- added weekday() + isoweekday()

## 0.0.7

- added floor() and ceil() methods

## 0.0.6

- added TS.as_iso_date_basic and as_iso_basic

## 0.0.5

- added TS.from_iso()

## 0.0.4

- added return typehint to TS.now()

## 0.0.3

- Lower the minimal typing-extensions version

## 0.0.2

- Added `pytz` as a direct dependency and expanded the README documentation.
- Added Python 3.6 package metadata and corrected the package keywords for timestamp utilities.

## 0.0.1

- Initial packaged release with the `TS` and `TSMsec` timestamp classes and setup scripts.
