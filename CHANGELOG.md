# Changelog

All notable changes to sqlmesh-gizmosql will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.5] - 2026-05-10

### Changed

- Surface the matching `## [X.Y.Z]` section from `CHANGELOG.md` as the
  GitHub Release body, mirroring the convention used in the upstream
  [GizmoSQL](https://github.com/gizmodata/gizmosql) repo. The CI release
  job now extracts release notes via `awk` and feeds them to
  `softprops/action-gh-release@v2` via `body_path`. If the matching
  section isn't found, the release is still created (with auto-generated
  notes) and a CI warning is logged.

## [0.2.4] - 2026-05-10

### Changed

- Switched the integration-test fixture from an externally-managed
  GizmoSQL Docker container to the
  [`gizmosql`](https://pypi.org/project/gizmosql/) PyPI package's
  managed subprocess. The duplicate `gizmosql_config` /
  `gizmosql_adapter` fixtures from each test module are consolidated
  into `tests/integration/conftest.py`; the docker-compose / CI
  services / Makefile targets / `wait-for-gizmosql.sh` script for the
  GizmoSQL container are removed. PostgreSQL is still spun up via
  docker-compose / CI services for the DuckLake tests.
- Bumped `sqlmesh` minimum version to `>=0.234.0`.
- Bumped `adbc-driver-gizmosql` minimum version to `>=1.1.6`.
- Added `gizmosql` to dev extras as the new test-fixture driver.

## [0.2.3] - 2026-04-11

### Changed

- Swapped the PyPI download-count badge to a `pepy.tech`-backed
  endpoint.

## [0.2.2] - 2026-04-11

### Changed

- Bumped `sqlmesh` minimum version from `>=0.100.0` to `>=0.232.0`.
- Removed the post-DDL/DML `fetchall()` workaround now that the ADBC
  driver handles execution itself (#5).

## [0.2.1] - 2026-04-11

### Changed

- Bumped `adbc-driver-gizmosql` minimum version from `>=1.1.3` to
  `>=1.1.5` (#3).

## [0.2.0] - 2026-03-30

### Changed

- Migrated from a Flight-SQL-direct client to
  `adbc-driver-gizmosql`'s `dbapi.connect()`.
- Added `auth_type="external"` for OAuth/SSO authentication.
- Fixed schema auto-creation for tables / CTAS in non-default
  catalogs.
- Added DuckLake integration tests covering multi-catalog scenarios
  (DuckDB ATTACH and DuckLake-on-PostgreSQL).
