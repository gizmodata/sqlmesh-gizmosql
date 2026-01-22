"""
SQLMesh GizmoSQL Adapter.

This package provides GizmoSQL support for SQLMesh. When imported, it automatically
registers the GizmoSQL engine adapter and connection configuration with SQLMesh.

Usage:
    # Option 1: Explicit import (recommended for clarity)
    import sqlmesh_gizmosql
    from sqlmesh import Context

    # Option 2: Just import - registration happens automatically
    import sqlmesh_gizmosql  # noqa: F401

    # Then use SQLMesh normally with type: gizmosql in your config

Example config.yaml:
    gateways:
      my_gizmosql:
        connection:
          type: gizmosql
          host: localhost
          port: 31337
          username: user
          password: pass
          database: my_database
"""
from sqlmesh_gizmosql.adapter import GizmoSQLEngineAdapter
from sqlmesh_gizmosql.connection import GizmoSQLConnectionConfig

__version__ = "0.1.5"
__all__ = ["GizmoSQLEngineAdapter", "GizmoSQLConnectionConfig", "register", "__version__"]

_registered = False


def register() -> None:
    """
    Register GizmoSQL adapter and connection config with SQLMesh.

    This function is called automatically when the module is imported,
    but can also be called explicitly if needed.
    """
    global _registered
    if _registered:
        return

    # Register the engine adapter
    from sqlmesh.core import engine_adapter
    if "gizmosql" not in engine_adapter.DIALECT_TO_ENGINE_ADAPTER:
        engine_adapter.DIALECT_TO_ENGINE_ADAPTER["gizmosql"] = GizmoSQLEngineAdapter

    # Register the connection config
    from sqlmesh.core.config import connection as conn_module
    if "gizmosql" not in conn_module.CONNECTION_CONFIG_TO_TYPE:
        conn_module.CONNECTION_CONFIG_TO_TYPE["gizmosql"] = GizmoSQLConnectionConfig

    if "gizmosql" not in conn_module.DIALECT_TO_TYPE:
        conn_module.DIALECT_TO_TYPE["gizmosql"] = GizmoSQLConnectionConfig.DIALECT

    _registered = True


# Auto-register on import
register()
