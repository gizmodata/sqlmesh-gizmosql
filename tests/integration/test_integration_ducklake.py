"""Integration tests for GizmoSQL with DuckLake and multi-catalog support.

These tests require:
1. A running GizmoSQL server with DuckDB backend
2. PostgreSQL 16 running for DuckLake metadata storage

They are marked with the 'integration', 'docker', and 'ducklake' pytest markers.

To run:
    1. Start services: docker compose -f tests/integration/docker/compose.gizmosql.yaml up -d
    2. Wait for them: ./scripts/wait-for-gizmosql.sh
    3. Run tests: pytest tests/integration/test_integration_ducklake.py -v
"""

import typing as t

import pytest
from sqlglot import exp

from sqlmesh_gizmosql import GizmoSQLConnectionConfig, GizmoSQLEngineAdapter

pytestmark = [pytest.mark.integration, pytest.mark.docker, pytest.mark.ducklake]


@pytest.fixture(scope="module")
def gizmosql_config() -> GizmoSQLConnectionConfig:
    """Create a GizmoSQL connection config for testing.

    Environment variables can override defaults:
    - GIZMOSQL_HOST: hostname (default: localhost)
    - GIZMOSQL_PORT: port (default: 31337)
    - GIZMOSQL_USERNAME: username (default: gizmosql_username)
    - GIZMOSQL_PASSWORD: password (default: gizmosql_password)
    """
    import os

    return GizmoSQLConnectionConfig(
        host=os.environ.get("GIZMOSQL_HOST", "localhost"),
        port=int(os.environ.get("GIZMOSQL_PORT", "31337")),
        username=os.environ.get("GIZMOSQL_USERNAME", "gizmosql_username"),
        password=os.environ.get("GIZMOSQL_PASSWORD", "gizmosql_password"),
        use_encryption=True,
        disable_certificate_verification=True,
    )


@pytest.fixture(scope="module")
def gizmosql_adapter(gizmosql_config: GizmoSQLConnectionConfig) -> t.Generator[GizmoSQLEngineAdapter, None, None]:
    """Create a GizmoSQL engine adapter for testing."""
    adapter = gizmosql_config.create_engine_adapter()
    yield adapter
    adapter.close()


# =============================================================================
# Tests for Standard DuckDB Non-Default Catalogs
# =============================================================================


class TestDuckDBNonDefaultCatalog:
    """Tests for creating schemas and tables in non-default DuckDB catalogs."""

    def _ensure_catalog_detached(self, adapter: GizmoSQLEngineAdapter, catalog_name: str) -> None:
        """Helper to ensure a catalog is detached before attaching."""
        try:
            adapter.execute(f"DETACH {catalog_name}")
        except Exception:
            pass  # Catalog might not exist, that's OK

    def _attach_memory_catalog(self, adapter: GizmoSQLEngineAdapter, catalog_name: str) -> None:
        """Helper to attach an in-memory catalog, detaching first if needed."""
        self._ensure_catalog_detached(adapter, catalog_name)
        adapter.execute(f"ATTACH ':memory:' AS {catalog_name}")

    def test_create_database_catalog(self, gizmosql_adapter: GizmoSQLEngineAdapter):
        """Test creating a new DuckDB database (catalog) using ATTACH."""
        catalog_name = "test_create_cat"

        try:
            # Create the catalog using ATTACH (DuckDB syntax for creating additional catalogs)
            self._attach_memory_catalog(gizmosql_adapter, catalog_name)

            # Verify it exists
            result = gizmosql_adapter.fetchall("SELECT database_name FROM duckdb_databases()")
            catalog_names = [row[0] for row in result]
            assert catalog_name in catalog_names

        finally:
            self._ensure_catalog_detached(gizmosql_adapter, catalog_name)

    def test_create_schema_in_non_default_catalog(self, gizmosql_adapter: GizmoSQLEngineAdapter):
        """Test creating a schema in a non-default catalog."""
        catalog_name = "test_schema_cat"
        schema_name = "custom_schema"

        try:
            # Create the catalog using ATTACH
            self._attach_memory_catalog(gizmosql_adapter, catalog_name)

            # Create schema in the non-default catalog
            gizmosql_adapter.execute(f"CREATE SCHEMA {catalog_name}.{schema_name}")

            # Verify the schema exists
            result = gizmosql_adapter.fetchone(
                f"""
                SELECT schema_name
                FROM information_schema.schemata
                WHERE catalog_name = '{catalog_name}' AND schema_name = '{schema_name}'
                """
            )
            assert result is not None
            assert result[0] == schema_name

        finally:
            self._ensure_catalog_detached(gizmosql_adapter, catalog_name)

    def test_create_table_in_non_default_catalog(self, gizmosql_adapter: GizmoSQLEngineAdapter):
        """Test creating a table in a non-default catalog."""
        catalog_name = "test_table_cat"
        schema_name = "test_schema"
        table_name = f"{catalog_name}.{schema_name}.test_table"

        try:
            # Create the catalog using ATTACH
            self._attach_memory_catalog(gizmosql_adapter, catalog_name)

            # Create schema and table using adapter methods
            gizmosql_adapter.execute(f"CREATE SCHEMA {catalog_name}.{schema_name}")

            columns_to_types = {
                "id": exp.DataType.build("INT"),
                "name": exp.DataType.build("VARCHAR"),
                "value": exp.DataType.build("DOUBLE"),
            }
            gizmosql_adapter.create_table(table_name, columns_to_types)

            # Insert data
            gizmosql_adapter.execute(
                f"INSERT INTO {table_name} (id, name, value) VALUES (1, 'test', 3.14)"
            )

            # Query data
            result = gizmosql_adapter.fetchone(f"SELECT * FROM {table_name}")
            assert result is not None
            assert result[0] == 1
            assert result[1] == "test"
            assert abs(result[2] - 3.14) < 0.001

        finally:
            self._ensure_catalog_detached(gizmosql_adapter, catalog_name)

    def test_table_exists_in_non_default_catalog(self, gizmosql_adapter: GizmoSQLEngineAdapter):
        """Test checking if a table exists in a non-default catalog."""
        catalog_name = "test_exists_cat"
        schema_name = "exists_test_schema"
        table_name = f"{catalog_name}.{schema_name}.exists_test_table"

        try:
            # Create the catalog using ATTACH and schema
            self._attach_memory_catalog(gizmosql_adapter, catalog_name)
            gizmosql_adapter.execute(f"CREATE SCHEMA {catalog_name}.{schema_name}")

            # Table should not exist yet
            assert not gizmosql_adapter.table_exists(exp.to_table(table_name))

            # Create table
            columns_to_types = {"id": exp.DataType.build("INT")}
            gizmosql_adapter.create_table(table_name, columns_to_types)

            # Table should exist now
            assert gizmosql_adapter.table_exists(exp.to_table(table_name))

        finally:
            self._ensure_catalog_detached(gizmosql_adapter, catalog_name)

    def test_ctas_in_non_default_catalog(self, gizmosql_adapter: GizmoSQLEngineAdapter):
        """Test CREATE TABLE AS SELECT in a non-default catalog."""
        catalog_name = "test_ctas_cat"
        schema_name = "ctas_schema"
        table_name = f"{catalog_name}.{schema_name}.ctas_table"

        try:
            # Create the catalog using ATTACH and schema
            self._attach_memory_catalog(gizmosql_adapter, catalog_name)
            gizmosql_adapter.execute(f"CREATE SCHEMA {catalog_name}.{schema_name}")

            # Use CTAS
            columns_to_types = {
                "id": exp.DataType.build("INT"),
                "value": exp.DataType.build("VARCHAR"),
            }
            query = exp.select(
                exp.Literal.number(1).as_("id"),
                exp.Literal.string("hello").as_("value"),
            )
            gizmosql_adapter.ctas(table_name, query, columns_to_types)

            # Verify data
            result = gizmosql_adapter.fetchone(f"SELECT * FROM {table_name}")
            assert result is not None
            assert result[0] == 1
            assert result[1] == "hello"

        finally:
            self._ensure_catalog_detached(gizmosql_adapter, catalog_name)

    def test_use_catalog_switching(self, gizmosql_adapter: GizmoSQLEngineAdapter):
        """Test switching between catalogs with USE statement."""
        catalog_name = "test_switch_cat"

        try:
            # Create the secondary catalog using ATTACH
            self._attach_memory_catalog(gizmosql_adapter, catalog_name)

            # Get original catalog
            original_catalog = gizmosql_adapter.get_current_catalog()
            assert original_catalog is not None

            # Switch to secondary catalog
            gizmosql_adapter.set_current_catalog(catalog_name)
            current = gizmosql_adapter.get_current_catalog()
            assert current == catalog_name

            # Switch back
            gizmosql_adapter.set_current_catalog(original_catalog)
            current = gizmosql_adapter.get_current_catalog()
            assert current == original_catalog

        finally:
            self._ensure_catalog_detached(gizmosql_adapter, catalog_name)

    def test_auto_create_schema_in_non_default_catalog(self, gizmosql_adapter: GizmoSQLEngineAdapter):
        """Test that create_table auto-creates schema in non-default catalog.

        This tests the scenario where SQLMesh tries to create a table like:
        CREATE TABLE "other_catalog"."sqlmesh__duck"."table" ...

        The adapter should automatically create the schema in the correct catalog.
        """
        catalog_name = "test_auto_schema_cat"
        schema_name = "auto_created_schema"
        table_name = f"{catalog_name}.{schema_name}.auto_test_table"

        try:
            # Create the catalog but NOT the schema
            self._attach_memory_catalog(gizmosql_adapter, catalog_name)

            # Verify schema does NOT exist
            result = gizmosql_adapter.fetchone(
                f"""
                SELECT schema_name FROM information_schema.schemata
                WHERE catalog_name = '{catalog_name}' AND schema_name = '{schema_name}'
                """
            )
            assert result is None, "Schema should not exist before test"

            # Create table - this should auto-create the schema in the correct catalog
            columns_to_types = {
                "id": exp.DataType.build("INT"),
                "name": exp.DataType.build("VARCHAR"),
            }
            gizmosql_adapter.create_table(table_name, columns_to_types)

            # Verify schema was auto-created in the correct catalog
            result = gizmosql_adapter.fetchone(
                f"""
                SELECT schema_name FROM information_schema.schemata
                WHERE catalog_name = '{catalog_name}' AND schema_name = '{schema_name}'
                """
            )
            assert result is not None, "Schema should have been auto-created in the non-default catalog"
            assert result[0] == schema_name

            # Verify table is usable
            gizmosql_adapter.execute(f"INSERT INTO {table_name} (id, name) VALUES (1, 'test')")
            result = gizmosql_adapter.fetchone(f"SELECT * FROM {table_name}")
            assert result is not None
            assert result[0] == 1

        finally:
            self._ensure_catalog_detached(gizmosql_adapter, catalog_name)

    def test_ctas_auto_create_schema_in_non_default_catalog(self, gizmosql_adapter: GizmoSQLEngineAdapter):
        """Test that CTAS auto-creates schema in non-default catalog.

        This tests the exact scenario from the customer error:
        CREATE TABLE IF NOT EXISTS "catalog"."sqlmesh__duck"."table" AS SELECT ...
        """
        catalog_name = "test_ctas_auto_cat"
        schema_name = "sqlmesh__duck"  # Use the actual SQLMesh schema name
        table_name = f"{catalog_name}.{schema_name}.ctas_auto_table"

        try:
            # Create the catalog but NOT the schema
            self._attach_memory_catalog(gizmosql_adapter, catalog_name)

            # Verify schema does NOT exist
            result = gizmosql_adapter.fetchone(
                f"""
                SELECT schema_name FROM information_schema.schemata
                WHERE catalog_name = '{catalog_name}' AND schema_name = '{schema_name}'
                """
            )
            assert result is None, "Schema should not exist before test"

            # Use CTAS - this should auto-create the schema in the correct catalog
            columns_to_types = {
                "id": exp.DataType.build("INT"),
                "value": exp.DataType.build("VARCHAR"),
            }
            query = exp.select(
                exp.Literal.number(42).as_("id"),
                exp.Literal.string("auto_created").as_("value"),
            )
            gizmosql_adapter.ctas(table_name, query, columns_to_types)

            # Verify schema was auto-created
            result = gizmosql_adapter.fetchone(
                f"""
                SELECT schema_name FROM information_schema.schemata
                WHERE catalog_name = '{catalog_name}' AND schema_name = '{schema_name}'
                """
            )
            assert result is not None, "Schema should have been auto-created"

            # Verify data
            result = gizmosql_adapter.fetchone(f"SELECT * FROM {table_name}")
            assert result is not None
            assert result[0] == 42
            assert result[1] == "auto_created"

        finally:
            self._ensure_catalog_detached(gizmosql_adapter, catalog_name)


# =============================================================================
# Tests for DuckLake Extension with PostgreSQL Metadata
# =============================================================================


class TestDuckLake:
    """Tests for DuckLake extension with PostgreSQL metadata storage."""

    @pytest.fixture(scope="class")
    def ducklake_setup(self, gizmosql_adapter: GizmoSQLEngineAdapter) -> t.Generator[str, None, None]:
        """Setup DuckLake with PostgreSQL metadata backend."""
        import os

        ducklake_catalog = "my_ducklake"
        postgres_host = os.environ.get("POSTGRES_HOST", "postgres")
        postgres_port = os.environ.get("POSTGRES_PORT", "5432")
        postgres_db = os.environ.get("POSTGRES_DB", "ducklake_catalog")
        postgres_user = os.environ.get("POSTGRES_USER", "postgres")
        postgres_password = os.environ.get("POSTGRES_PASSWORD", "mysecretpassword")

        try:
            # Install required extensions
            gizmosql_adapter.execute("INSTALL ducklake")
            gizmosql_adapter.execute("INSTALL postgres")
            gizmosql_adapter.execute("LOAD ducklake")
            gizmosql_adapter.execute("LOAD postgres")

            # Create PostgreSQL secret for DuckLake metadata
            gizmosql_adapter.execute(f"""
                CREATE OR REPLACE SECRET postgres_secret (
                    TYPE postgres,
                    HOST '{postgres_host}',
                    PORT {postgres_port},
                    DATABASE '{postgres_db}',
                    USER '{postgres_user}',
                    PASSWORD '{postgres_password}'
                )
            """)

            # Create DuckLake secret (use /tmp for data storage in container)
            gizmosql_adapter.execute("""
                CREATE OR REPLACE SECRET ducklake_secret (
                    TYPE DUCKLAKE,
                    METADATA_PATH '',
                    DATA_PATH '/tmp/ducklake/',
                    METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'postgres_secret'}
                )
            """)

            # Attach DuckLake catalog
            gizmosql_adapter.execute(f"ATTACH 'ducklake:ducklake_secret' AS {ducklake_catalog}")

            yield ducklake_catalog

        finally:
            # Cleanup
            try:
                gizmosql_adapter.execute(f"DETACH {ducklake_catalog}")
            except Exception:
                pass

    def test_ducklake_attach(self, gizmosql_adapter: GizmoSQLEngineAdapter, ducklake_setup: str):
        """Test that DuckLake catalog is properly attached."""
        ducklake_catalog = ducklake_setup

        # Check that the catalog appears in the list
        result = gizmosql_adapter.fetchall("SELECT database_name FROM duckdb_databases()")
        catalog_names = [row[0] for row in result]
        assert ducklake_catalog in catalog_names

    def test_create_schema_in_ducklake(self, gizmosql_adapter: GizmoSQLEngineAdapter, ducklake_setup: str):
        """Test creating a schema in the DuckLake catalog."""
        ducklake_catalog = ducklake_setup
        schema_name = "dl_test_schema"

        try:
            # Create schema in DuckLake catalog
            gizmosql_adapter.execute(f"CREATE SCHEMA IF NOT EXISTS {ducklake_catalog}.{schema_name}")

            # Verify it exists
            result = gizmosql_adapter.fetchone(
                f"""
                SELECT schema_name
                FROM information_schema.schemata
                WHERE catalog_name = '{ducklake_catalog}' AND schema_name = '{schema_name}'
                """
            )
            assert result is not None
            assert result[0] == schema_name

        finally:
            try:
                gizmosql_adapter.execute(f"DROP SCHEMA IF EXISTS {ducklake_catalog}.{schema_name} CASCADE")
            except Exception:
                pass

    def test_create_table_in_ducklake(self, gizmosql_adapter: GizmoSQLEngineAdapter, ducklake_setup: str):
        """Test creating a table in the DuckLake catalog."""
        ducklake_catalog = ducklake_setup
        schema_name = "dl_table_schema"
        table_name = f"{ducklake_catalog}.{schema_name}.dl_test_table"

        try:
            # Create schema
            gizmosql_adapter.execute(f"CREATE SCHEMA IF NOT EXISTS {ducklake_catalog}.{schema_name}")

            # Create table
            columns_to_types = {
                "id": exp.DataType.build("INT"),
                "name": exp.DataType.build("VARCHAR"),
                "created_at": exp.DataType.build("TIMESTAMP"),
            }
            gizmosql_adapter.create_table(table_name, columns_to_types)

            # Insert data
            gizmosql_adapter.execute(
                f"INSERT INTO {table_name} (id, name, created_at) VALUES (1, 'ducklake_test', '2024-01-01 12:00:00')"
            )

            # Query data
            result = gizmosql_adapter.fetchone(f"SELECT id, name FROM {table_name}")
            assert result is not None
            assert result[0] == 1
            assert result[1] == "ducklake_test"

        finally:
            try:
                gizmosql_adapter.execute(f"DROP SCHEMA IF EXISTS {ducklake_catalog}.{schema_name} CASCADE")
            except Exception:
                pass

    def test_ctas_in_ducklake(self, gizmosql_adapter: GizmoSQLEngineAdapter, ducklake_setup: str):
        """Test CREATE TABLE AS SELECT in DuckLake catalog."""
        ducklake_catalog = ducklake_setup
        schema_name = "dl_ctas_schema"
        table_name = f"{ducklake_catalog}.{schema_name}.dl_ctas_table"

        try:
            # Create schema
            gizmosql_adapter.execute(f"CREATE SCHEMA IF NOT EXISTS {ducklake_catalog}.{schema_name}")

            # Use CTAS
            columns_to_types = {
                "id": exp.DataType.build("INT"),
                "value": exp.DataType.build("VARCHAR"),
            }
            query = exp.select(
                exp.Literal.number(42).as_("id"),
                exp.Literal.string("ducklake_ctas").as_("value"),
            )
            gizmosql_adapter.ctas(table_name, query, columns_to_types)

            # Verify data
            result = gizmosql_adapter.fetchone(f"SELECT * FROM {table_name}")
            assert result is not None
            assert result[0] == 42
            assert result[1] == "ducklake_ctas"

        finally:
            try:
                gizmosql_adapter.execute(f"DROP SCHEMA IF EXISTS {ducklake_catalog}.{schema_name} CASCADE")
            except Exception:
                pass

    def test_table_exists_in_ducklake(self, gizmosql_adapter: GizmoSQLEngineAdapter, ducklake_setup: str):
        """Test checking if a table exists in DuckLake catalog."""
        ducklake_catalog = ducklake_setup
        schema_name = "dl_exists_schema"
        table_name = f"{ducklake_catalog}.{schema_name}.dl_exists_table"

        try:
            # Create schema
            gizmosql_adapter.execute(f"CREATE SCHEMA IF NOT EXISTS {ducklake_catalog}.{schema_name}")

            # Table should not exist yet
            assert not gizmosql_adapter.table_exists(exp.to_table(table_name))

            # Create table
            columns_to_types = {"id": exp.DataType.build("INT")}
            gizmosql_adapter.create_table(table_name, columns_to_types)

            # Table should exist now
            assert gizmosql_adapter.table_exists(exp.to_table(table_name))

        finally:
            try:
                gizmosql_adapter.execute(f"DROP SCHEMA IF EXISTS {ducklake_catalog}.{schema_name} CASCADE")
            except Exception:
                pass

    def test_switch_to_ducklake_catalog(self, gizmosql_adapter: GizmoSQLEngineAdapter, ducklake_setup: str):
        """Test switching to DuckLake catalog with USE statement."""
        ducklake_catalog = ducklake_setup

        # Get original catalog
        original_catalog = gizmosql_adapter.get_current_catalog()

        # Switch to DuckLake
        gizmosql_adapter.set_current_catalog(ducklake_catalog)
        current = gizmosql_adapter.get_current_catalog()
        assert current == ducklake_catalog

        # Switch back
        gizmosql_adapter.set_current_catalog(original_catalog)

    def test_dataframe_bulk_ingestion_to_ducklake(self, gizmosql_adapter: GizmoSQLEngineAdapter, ducklake_setup: str):
        """Test bulk DataFrame ingestion into a DuckLake table."""
        import pandas as pd

        ducklake_catalog = ducklake_setup
        schema_name = "dl_bulk_schema"
        table_name = f"{ducklake_catalog}.{schema_name}.dl_bulk_table"

        try:
            # Create schema
            gizmosql_adapter.execute(f"CREATE SCHEMA IF NOT EXISTS {ducklake_catalog}.{schema_name}")

            # Create a test DataFrame
            df = pd.DataFrame({
                "id": [1, 2, 3, 4, 5],
                "name": ["alice", "bob", "charlie", "diana", "eve"],
                "score": [85.5, 92.0, 78.5, 95.0, 88.5],
            })

            # Create target table
            columns_to_types = {
                "id": exp.DataType.build("INT"),
                "name": exp.DataType.build("VARCHAR"),
                "score": exp.DataType.build("DOUBLE"),
            }
            gizmosql_adapter.create_table(table_name, columns_to_types)

            # Use replace_query with DataFrame
            gizmosql_adapter.replace_query(
                table_name,
                df,
                columns_to_types,
            )

            # Verify data was loaded
            result = gizmosql_adapter.fetchall(f"SELECT * FROM {table_name} ORDER BY id")
            assert len(result) == 5
            assert result[0][0] == 1
            assert result[0][1] == "alice"

        finally:
            try:
                gizmosql_adapter.execute(f"DROP SCHEMA IF EXISTS {ducklake_catalog}.{schema_name} CASCADE")
            except Exception:
                pass

    def test_auto_create_schema_in_ducklake(self, gizmosql_adapter: GizmoSQLEngineAdapter, ducklake_setup: str):
        """Test that create_table auto-creates schema in DuckLake catalog.

        This tests the exact customer scenario where SQLMesh tries to create:
        CREATE TABLE "ducklake_catalog"."sqlmesh__duck"."table" ...
        but the schema doesn't exist yet.
        """
        ducklake_catalog = ducklake_setup
        schema_name = "sqlmesh__duck"  # Use the actual SQLMesh schema name
        table_name = f"{ducklake_catalog}.{schema_name}.dl_auto_table"

        try:
            # Ensure schema does NOT exist
            try:
                gizmosql_adapter.execute(f"DROP SCHEMA IF EXISTS {ducklake_catalog}.{schema_name} CASCADE")
            except Exception:
                pass

            # Verify schema doesn't exist
            result = gizmosql_adapter.fetchone(
                f"""
                SELECT schema_name FROM information_schema.schemata
                WHERE catalog_name = '{ducklake_catalog}' AND schema_name = '{schema_name}'
                """
            )
            assert result is None, "Schema should not exist before test"

            # Create table - this should auto-create the schema in DuckLake
            columns_to_types = {
                "id": exp.DataType.build("INT"),
                "name": exp.DataType.build("VARCHAR"),
            }
            gizmosql_adapter.create_table(table_name, columns_to_types)

            # Verify schema was auto-created in DuckLake
            result = gizmosql_adapter.fetchone(
                f"""
                SELECT schema_name FROM information_schema.schemata
                WHERE catalog_name = '{ducklake_catalog}' AND schema_name = '{schema_name}'
                """
            )
            assert result is not None, "Schema should have been auto-created in DuckLake catalog"

            # Verify table is usable
            gizmosql_adapter.execute(f"INSERT INTO {table_name} (id, name) VALUES (1, 'ducklake_auto')")
            result = gizmosql_adapter.fetchone(f"SELECT * FROM {table_name}")
            assert result is not None
            assert result[0] == 1
            assert result[1] == "ducklake_auto"

        finally:
            try:
                gizmosql_adapter.execute(f"DROP SCHEMA IF EXISTS {ducklake_catalog}.{schema_name} CASCADE")
            except Exception:
                pass

    def test_ctas_auto_create_schema_in_ducklake(self, gizmosql_adapter: GizmoSQLEngineAdapter, ducklake_setup: str):
        """Test that CTAS auto-creates schema in DuckLake catalog.

        This is the exact error scenario from the customer:
        CREATE TABLE IF NOT EXISTS "orennia_ops_bronze"."sqlmesh__duck"."table" AS SELECT ...
        Error: Schema "sqlmesh__duck" not found in DuckLakeCatalog
        """
        ducklake_catalog = ducklake_setup
        schema_name = "sqlmesh__duck_ctas"
        table_name = f"{ducklake_catalog}.{schema_name}.dl_ctas_auto_table"

        try:
            # Ensure schema does NOT exist
            try:
                gizmosql_adapter.execute(f"DROP SCHEMA IF EXISTS {ducklake_catalog}.{schema_name} CASCADE")
            except Exception:
                pass

            # Verify schema doesn't exist
            result = gizmosql_adapter.fetchone(
                f"""
                SELECT schema_name FROM information_schema.schemata
                WHERE catalog_name = '{ducklake_catalog}' AND schema_name = '{schema_name}'
                """
            )
            assert result is None, "Schema should not exist before test"

            # Use CTAS - this should auto-create the schema in DuckLake
            columns_to_types = {
                "id": exp.DataType.build("INT"),
                "value": exp.DataType.build("VARCHAR"),
            }
            query = exp.select(
                exp.Literal.number(99).as_("id"),
                exp.Literal.string("ducklake_ctas_auto").as_("value"),
            )
            gizmosql_adapter.ctas(table_name, query, columns_to_types)

            # Verify schema was auto-created
            result = gizmosql_adapter.fetchone(
                f"""
                SELECT schema_name FROM information_schema.schemata
                WHERE catalog_name = '{ducklake_catalog}' AND schema_name = '{schema_name}'
                """
            )
            assert result is not None, "Schema should have been auto-created in DuckLake"

            # Verify data
            result = gizmosql_adapter.fetchone(f"SELECT * FROM {table_name}")
            assert result is not None
            assert result[0] == 99
            assert result[1] == "ducklake_ctas_auto"

        finally:
            try:
                gizmosql_adapter.execute(f"DROP SCHEMA IF EXISTS {ducklake_catalog}.{schema_name} CASCADE")
            except Exception:
                pass


# =============================================================================
# Tests for Cross-Catalog Operations
# =============================================================================


class TestCrossCatalogOperations:
    """Tests for operations that span multiple catalogs."""

    def _ensure_catalog_detached(self, adapter: GizmoSQLEngineAdapter, catalog_name: str) -> None:
        """Helper to ensure a catalog is detached."""
        try:
            adapter.execute(f"DETACH {catalog_name}")
        except Exception:
            pass

    def _attach_memory_catalog(self, adapter: GizmoSQLEngineAdapter, catalog_name: str) -> None:
        """Helper to attach an in-memory catalog, detaching first if needed."""
        self._ensure_catalog_detached(adapter, catalog_name)
        adapter.execute(f"ATTACH ':memory:' AS {catalog_name}")

    def test_query_across_catalogs(self, gizmosql_adapter: GizmoSQLEngineAdapter):
        """Test querying data across different catalogs."""
        catalog_a = "cross_cat_a"
        catalog_b = "cross_cat_b"

        try:
            # Create catalogs using ATTACH and schemas
            self._attach_memory_catalog(gizmosql_adapter, catalog_a)
            self._attach_memory_catalog(gizmosql_adapter, catalog_b)
            gizmosql_adapter.execute(f"CREATE SCHEMA {catalog_a}.schema_a")
            gizmosql_adapter.execute(f"CREATE SCHEMA {catalog_b}.schema_b")

            # Create table in catalog_a
            gizmosql_adapter.execute(f"""
                CREATE TABLE {catalog_a}.schema_a.users (
                    id INT,
                    name VARCHAR
                )
            """)
            gizmosql_adapter.execute(f"""
                INSERT INTO {catalog_a}.schema_a.users VALUES (1, 'alice'), (2, 'bob')
            """)

            # Create table in catalog_b
            gizmosql_adapter.execute(f"""
                CREATE TABLE {catalog_b}.schema_b.orders (
                    id INT,
                    user_id INT,
                    amount DOUBLE
                )
            """)
            gizmosql_adapter.execute(f"""
                INSERT INTO {catalog_b}.schema_b.orders VALUES (1, 1, 100.0), (2, 2, 200.0)
            """)

            # Query across catalogs with JOIN
            result = gizmosql_adapter.fetchall(f"""
                SELECT u.name, o.amount
                FROM {catalog_a}.schema_a.users u
                JOIN {catalog_b}.schema_b.orders o ON u.id = o.user_id
                ORDER BY u.name
            """)

            assert len(result) == 2
            assert result[0][0] == "alice"
            assert result[0][1] == 100.0
            assert result[1][0] == "bob"
            assert result[1][1] == 200.0

        finally:
            self._ensure_catalog_detached(gizmosql_adapter, catalog_a)
            self._ensure_catalog_detached(gizmosql_adapter, catalog_b)

    def test_insert_from_different_catalog(self, gizmosql_adapter: GizmoSQLEngineAdapter):
        """Test inserting data from one catalog into another."""
        catalog_a = "insert_cat_a"
        catalog_b = "insert_cat_b"

        try:
            # Create catalogs using ATTACH and schemas
            self._attach_memory_catalog(gizmosql_adapter, catalog_a)
            self._attach_memory_catalog(gizmosql_adapter, catalog_b)
            gizmosql_adapter.execute(f"CREATE SCHEMA {catalog_a}.schema_a")
            gizmosql_adapter.execute(f"CREATE SCHEMA {catalog_b}.schema_b")

            # Create source table in catalog_a
            gizmosql_adapter.execute(f"""
                CREATE TABLE {catalog_a}.schema_a.source_data (
                    id INT,
                    value VARCHAR
                )
            """)
            gizmosql_adapter.execute(f"""
                INSERT INTO {catalog_a}.schema_a.source_data VALUES (1, 'x'), (2, 'y')
            """)

            # Create target table in catalog_b
            gizmosql_adapter.execute(f"""
                CREATE TABLE {catalog_b}.schema_b.target_data (
                    id INT,
                    value VARCHAR
                )
            """)

            # Insert from catalog_a into catalog_b
            gizmosql_adapter.execute(f"""
                INSERT INTO {catalog_b}.schema_b.target_data
                SELECT * FROM {catalog_a}.schema_a.source_data
            """)

            # Verify
            result = gizmosql_adapter.fetchall(f"SELECT * FROM {catalog_b}.schema_b.target_data ORDER BY id")
            assert len(result) == 2
            assert result[0][1] == "x"
            assert result[1][1] == "y"

        finally:
            self._ensure_catalog_detached(gizmosql_adapter, catalog_a)
            self._ensure_catalog_detached(gizmosql_adapter, catalog_b)
