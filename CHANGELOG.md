## dbt-spark 1.3.0b2 (Release TBD)

## dbt-spark 1.3.0b1 (July 29, 2022)

### Features
- Support python model through notebook, currently supported materializations are table and incremental. ([#377](https://github.com/dbt-labs/dbt-spark/pull/377))

### Fixes
- Pin `pyodbc` to version 4.0.32 to prevent overwriting `libodbc.so` and `libltdl.so` on Linux ([#397](https://github.com/dbt-labs/dbt-spark/issues/397/), [#398](https://github.com/dbt-labs/dbt-spark/pull/398/))

### Under the hood
- Support core incremental refactor ([#394](https://github.com/dbt-labs/dbt-spark/issues/394))

### Contributors
- [@barberscott](https://github.com/barberscott)  ([#398](https://github.com/dbt-labs/dbt-spark/pull/398/))

## dbt-spark 1.2.0rc1 (July 12, 2022)

### Fixes
- Incremental materialization updated to not drop table first if full refresh for delta lake format, as it already runs _create or replace table_ ([#286](https://github.com/dbt-labs/dbt-spark/issues/286), [#287](https://github.com/dbt-labs/dbt-spark/pull/287/))
- Apache Spark version upgraded to 3.1.1 ([#348](https://github.com/dbt-labs/dbt-spark/issues/348), [#349](https://github.com/dbt-labs/dbt-spark/pull/349))

### Features
- Add grants to materializations ([#366](https://github.com/dbt-labs/dbt-spark/issues/366), [#381](https://github.com/dbt-labs/dbt-spark/pull/381))

### Under the hood
- Update `SparkColumn.numeric_type` to return `decimal` instead of `numeric`, since SparkSQL exclusively supports the former ([#380](https://github.com/dbt-labs/dbt-spark/pull/380))
- Make minimal changes to support dbt Core incremental materialization refactor ([#402](https://github.com/dbt-labs/dbt-spark/issue/402), [#394](httpe://github.com/dbt-labs/dbt-spark/pull/394))

### Contributors
- [@grindheim](https://github.com/grindheim) ([#287](https://github.com/dbt-labs/dbt-spark/pull/287/))
- [@nssalian](https://github.com/nssalian) ([#349](https://github.com/dbt-labs/dbt-spark/pull/349))

## dbt-spark 1.2.0b1 (June 24, 2022)

### Fixes
- `adapter.get_columns_in_relation` (method) and `get_columns_in_relation` (macro) now return identical responses. The previous behavior of `get_columns_in_relation` (macro) is now represented by a new macro, `get_columns_in_relation_raw` ([#354](https://github.com/dbt-labs/dbt-spark/issues/354), [#355](https://github.com/dbt-labs/dbt-spark/pull/355))

### Under the hood
- Initialize lift + shift for cross-db macros ([#359](https://github.com/dbt-labs/dbt-spark/pull/359))
- Add invocation env to user agent string ([#367](https://github.com/dbt-labs/dbt-spark/pull/367))
- Use dispatch pattern for get_columns_in_relation_raw macro ([#365](https://github.com/dbt-labs/dbt-spark/pull/365))

### Contributors
- [@ueshin](https://github.com/ueshin) ([#365](https://github.com/dbt-labs/dbt-spark/pull/365))
- [@dbeatty10](https://github.com/dbeatty10) ([#359](https://github.com/dbt-labs/dbt-spark/pull/359))

## dbt-spark 1.1.0 (April 28, 2022)

### Features
- Add session connection method ([#272](https://github.com/dbt-labs/dbt-spark/issues/272), [#279](https://github.com/dbt-labs/dbt-spark/pull/279))
- rename file to match reference to dbt-core ([#344](https://github.com/dbt-labs/dbt-spark/pull/344))

### Under the hood
- Add precommit tooling to this repo ([#356](https://github.com/dbt-labs/dbt-spark/pull/356))
- Use dbt.tests.adapter.basic in test suite ([#298](https://github.com/dbt-labs/dbt-spark/issues/298), [#299](https://github.com/dbt-labs/dbt-spark/pull/299))
- Make internal macros use macro dispatch to be overridable in child adapters ([#319](https://github.com/dbt-labs/dbt-spark/issues/319), [#320](https://github.com/dbt-labs/dbt-spark/pull/320))
- Override adapter method 'run_sql_for_tests' ([#323](https://github.com/dbt-labs/dbt-spark/issues/323), [#324](https://github.com/dbt-labs/dbt-spark/pull/324))
- when a table or view doesn't exist, 'adapter.get_columns_in_relation' will return empty list instead of fail ([#328]https://github.com/dbt-labs/dbt-spark/pull/328)

### Contributors
- [@JCZuurmond](https://github.com/dbt-labs/dbt-spark/pull/279) ( [#279](https://github.com/dbt-labs/dbt-spark/pull/279))
- [@ueshin](https://github.com/ueshin) ([#320](https://github.com/dbt-labs/dbt-spark/pull/320))

## dbt-spark 1.1.0b1 (March 23, 2022)

### Features
- Add session connection method ([#272](https://github.com/dbt-labs/dbt-spark/issues/272), [#279](https://github.com/dbt-labs/dbt-spark/pull/279))
- Adds new integration test to check against new ability to allow unique_key to be a list. ([#282](https://github.com/dbt-labs/dbt-spark/issues/282)), [#291](https://github.com/dbt-labs/dbt-spark/pull/291))

### Fixes
- Closes the connection properly ([#280](https://github.com/dbt-labs/dbt-spark/issues/280), [#285](https://github.com/dbt-labs/dbt-spark/pull/285))

### Under the hood
- Use dbt.tests.adapter.basic in test suite ([#298](https://github.com/dbt-labs/dbt-spark/issues/298), [#299](https://github.com/dbt-labs/dbt-spark/pull/299))
- Make internal macros use macro dispatch to be overridable in child adapters ([#319](https://github.com/dbt-labs/dbt-spark/issues/319), [#320](https://github.com/dbt-labs/dbt-spark/pull/320))
- Override adapter method 'run_sql_for_tests' ([#323](https://github.com/dbt-labs/dbt-spark/issues/323), [#324](https://github.com/dbt-labs/dbt-spark/pull/324))
- when a table or view doesn't exist, 'adapter.get_columns_in_relation' will return empty list instead of fail ([#328]https://github.com/dbt-labs/dbt-spark/pull/328)
- get_response -> AdapterResponse ([#265](https://github.com/dbt-labs/dbt-spark/pull/265))
- Adding stale Actions workflow ([#275](https://github.com/dbt-labs/dbt-spark/pull/275))
- Update plugin author name (`fishtown-analytics` &rarr; `dbt-labs`) in ODBC user agent ([#288](https://github.com/dbt-labs/dbt-spark/pull/288))
- Configure insert_overwrite models to use parquet ([#301](https://github.com/dbt-labs/dbt-spark/pull/301))

### Contributors
- [@JCZuurmond](https://github.com/dbt-labs/dbt-spark/pull/279) ([#279](https://github.com/dbt-labs/dbt-spark/pull/279))
- [@ueshin](https://github.com/ueshin) ([#320](https://github.com/dbt-labs/dbt-spark/pull/320))
- [@amychen1776](https://github.com/amychen1776) ([#288](https://github.com/dbt-labs/dbt-spark/pull/288))
- [@ueshin](https://github.com/ueshin) ([#285](https://github.com/dbt-labs/dbt-spark/pull/285))

## Previous Releases
For information on prior major and minor releases, see their changelogs:
- [1.0](https://github.com/dbt-labs/dbt-spark/blob/1.0.latest/CHANGELOG.md)
- [0.21](https://github.com/dbt-labs/dbt-spark/blob/0.21.latest/CHANGELOG.md)
- [0.20](https://github.com/dbt-labs/dbt-spark/blob/0.20.latest/CHANGELOG.md)
- [0.19 and earlier](https://github.com/dbt-labs/dbt-spark/blob/0.19.latest/CHANGELOG.md)
