"""Shared fixtures for integration tests.

The GizmoSQL server is started as a managed subprocess via the
[`gizmosql`](https://pypi.org/project/gizmosql/) PyPI package — Docker is
no longer required for the server itself. PostgreSQL is still expected
to be reachable at ``$POSTGRES_HOST:$POSTGRES_PORT`` for the DuckLake
tests; ``docker compose -f tests/integration/docker/compose.gizmosql.yaml
up -d`` still brings that up, and CI continues to launch postgres as a
service container.
"""

from __future__ import annotations

import typing as t

import gizmosql
import pytest

from sqlmesh_gizmosql import GizmoSQLConnectionConfig, GizmoSQLEngineAdapter


@pytest.fixture(scope="session")
def gizmosql_server() -> t.Generator[gizmosql.Server, None, None]:
    """Start a GizmoSQL server as a subprocess for the duration of the
    pytest session. Auto-picks a free port."""
    with gizmosql.Server(
        username="gizmosql_user",
        password="gizmosql_password",
    ) as srv:
        yield srv


@pytest.fixture(scope="session")
def gizmosql_config(gizmosql_server: gizmosql.Server) -> GizmoSQLConnectionConfig:
    """Connection config pointing at the test server."""
    return GizmoSQLConnectionConfig(
        host=gizmosql_server.host,
        port=gizmosql_server.port,
        username=gizmosql_server.username,
        password=gizmosql_server.password,
        use_encryption=False,
    )


@pytest.fixture(scope="session")
def gizmosql_adapter(
    gizmosql_config: GizmoSQLConnectionConfig,
) -> t.Generator[GizmoSQLEngineAdapter, None, None]:
    """Engine adapter for the test session."""
    adapter = gizmosql_config.create_engine_adapter()
    yield adapter
    adapter.close()
