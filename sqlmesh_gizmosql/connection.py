"""
GizmoSQL Connection Configuration for SQLMesh.

Provides the connection configuration class for GizmoSQL servers.
"""
from __future__ import annotations

import typing as t

from pydantic import Field
from sqlmesh.core.config.common import concurrent_tasks_validator
from sqlmesh.core.config.connection import ConnectionConfig
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.pydantic import model_validator

from sqlmesh_gizmosql.adapter import GizmoSQLEngineAdapter


def _gizmosql_import_validator(cls: t.Any, data: t.Any) -> t.Any:
    """Validate that ADBC Flight SQL driver is installed."""
    check_import = (
        data.pop("check_import", True) if isinstance(data, dict) else True
    )
    if not check_import:
        return data
    try:
        import adbc_driver_flightsql  # noqa: F401
    except ImportError:
        raise ConfigError(
            "Failed to import the 'adbc_driver_flightsql' library. "
            "Please install it with: pip install sqlmesh-gizmosql[gizmosql] "
            "or pip install adbc-driver-flightsql pyarrow"
        )
    return data


class GizmoSQLConnectionConfig(ConnectionConfig):
    """
    GizmoSQL connection configuration.

    GizmoSQL is a database server that uses DuckDB as its execution engine and
    exposes an Arrow Flight SQL interface for remote connections. This configuration
    uses ADBC (Arrow Database Connectivity) with the Flight SQL driver.

    Args:
        host: The hostname of the GizmoSQL server.
        port: The port of the GizmoSQL server (default: 31337).
        username: The username for authentication.
        password: The password for authentication.
        use_encryption: Whether to use TLS encryption (default: True).
        disable_certificate_verification: Whether to skip TLS certificate verification.
            Useful for self-signed certificates in development (default: False).
        database: The default database/catalog to use.
        concurrent_tasks: The maximum number of concurrent tasks.
        register_comments: Whether to register model comments.
        pre_ping: Whether to pre-ping the connection.
    """

    host: str = "localhost"
    port: int = 31337
    username: str
    password: str
    use_encryption: bool = True
    disable_certificate_verification: bool = False
    database: t.Optional[str] = None

    concurrent_tasks: int = 4
    register_comments: bool = True
    pre_ping: bool = False

    type_: t.Literal["gizmosql"] = Field(alias="type", default="gizmosql")
    DIALECT: t.ClassVar[t.Literal["duckdb"]] = "duckdb"
    DISPLAY_NAME: t.ClassVar[t.Literal["GizmoSQL"]] = "GizmoSQL"
    DISPLAY_ORDER: t.ClassVar[t.Literal[17]] = 17

    _engine_import_validator = model_validator(mode="before")(_gizmosql_import_validator)
    _concurrent_tasks_validator = concurrent_tasks_validator

    @property
    def _connection_kwargs_keys(self) -> t.Set[str]:
        # ADBC uses a different connection pattern, so we don't pass these directly
        return set()

    @property
    def _engine_adapter(self) -> t.Type[EngineAdapter]:
        return GizmoSQLEngineAdapter

    @property
    def _connection_factory(self) -> t.Callable:
        """
        Create a connection factory for GizmoSQL using ADBC Flight SQL driver.

        The connection is established using the Arrow Flight SQL protocol over gRPC.
        """
        import re

        from adbc_driver_flightsql import DatabaseOptions
        from adbc_driver_flightsql import dbapi as flightsql

        def connect() -> t.Any:
            # Build the URI for the Flight SQL connection
            protocol = "grpc+tls" if self.use_encryption else "grpc"
            uri = f"{protocol}://{self.host}:{self.port}"

            # ADBC database-level options (passed to the driver)
            db_kwargs: t.Dict[str, str] = {
                "username": self.username,
                "password": self.password,
            }

            # Add TLS skip verify option using the proper DatabaseOptions enum
            if self.use_encryption and self.disable_certificate_verification:
                db_kwargs[DatabaseOptions.TLS_SKIP_VERIFY.value] = "true"

            # Create the connection - uri is first positional arg, db_kwargs is for driver options
            # Explicit autocommit=True since GizmoSQL doesn't support manual transaction commits
            conn = flightsql.connect(uri, db_kwargs=db_kwargs, autocommit=True)

            # Verify the backend is DuckDB - this adapter only supports the DuckDB backend
            vendor_version = conn.adbc_get_info().get("vendor_version", "")
            if not re.search(pattern=r"^duckdb ", string=vendor_version):
                conn.close()
                raise ConfigError(
                    f"Unsupported GizmoSQL server backend: '{vendor_version}'. "
                    "This adapter only supports the DuckDB backend for GizmoSQL."
                )

            return conn

        return connect

    def get_catalog(self) -> t.Optional[str]:
        return self.database
