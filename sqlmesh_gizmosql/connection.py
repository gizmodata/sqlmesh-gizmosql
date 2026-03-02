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
    """Validate that ADBC GizmoSQL driver is installed."""
    check_import = data.pop("check_import", True) if isinstance(data, dict) else True
    if not check_import:
        return data
    try:
        import adbc_driver_gizmosql  # noqa: F401
    except ImportError:
        raise ConfigError(
            "Failed to import the 'adbc_driver_gizmosql' library. "
            "Please install it with: pip install sqlmesh-gizmosql "
            "or pip install adbc-driver-gizmosql"
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
        username: The username for authentication (not needed with auth_type="external").
        password: The password for authentication (not needed with auth_type="external").
        use_encryption: Whether to use TLS encryption (default: True).
        disable_certificate_verification: Whether to skip TLS certificate verification.
            Useful for self-signed certificates in development (default: False).
        auth_type: Authentication type (e.g., "external" for browser-based OAuth/SSO).
        database: The default database/catalog to use.
        concurrent_tasks: The maximum number of concurrent tasks.
        register_comments: Whether to register model comments.
        pre_ping: Whether to pre-ping the connection.
    """

    host: str = "localhost"
    port: int = 31337
    username: t.Optional[str] = None
    password: t.Optional[str] = None
    use_encryption: bool = True
    disable_certificate_verification: bool = False
    auth_type: t.Optional[str] = None
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
        Create a connection factory for GizmoSQL using adbc-driver-gizmosql.

        The connection is established using the Arrow Flight SQL protocol over gRPC.
        """
        import re

        from adbc_driver_gizmosql import dbapi as gizmosql

        def connect() -> t.Any:
            # Build the URI for the Flight SQL connection
            protocol = "grpc+tls" if self.use_encryption else "grpc"
            uri = f"{protocol}://{self.host}:{self.port}"

            connect_kwargs: t.Dict[str, t.Any] = {"uri": uri}
            if self.auth_type:
                connect_kwargs["auth_type"] = self.auth_type
            if self.username:
                connect_kwargs["username"] = self.username
                connect_kwargs["password"] = self.password or ""
            if self.use_encryption and self.disable_certificate_verification:
                connect_kwargs["tls_skip_verify"] = True

            conn = gizmosql.connect(**connect_kwargs)

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
