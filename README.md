# SQLMesh GizmoSQL Adapter

A [SQLMesh](https://sqlmesh.com) engine adapter for [GizmoSQL](https://github.com/gizmodata/gizmosql) - a database server that uses DuckDB as its execution engine and exposes an Arrow Flight SQL interface for remote connections.

## Installation

```bash
pip install sqlmesh-gizmosql
```

This will install `sqlmesh`, `adbc-driver-flightsql`, and `pyarrow` as dependencies.

## Usage

### 1. Import the adapter

Simply import the package before using SQLMesh. The adapter registers itself automatically:

```python
import sqlmesh_gizmosql  # Registers GizmoSQL adapter
from sqlmesh import Context

context = Context(paths="path/to/project")
```

### 2. Configure your connection

Add a GizmoSQL connection to your `config.yaml`:

```yaml
gateways:
  my_gizmosql:
    connection:
      type: gizmosql
      host: localhost
      port: 31337
      username: your_username
      password: your_password
      database: my_database  # optional, default catalog
      use_encryption: true   # default: true (uses TLS)
      disable_certificate_verification: false  # for self-signed certs
```

### 3. Use SQLMesh as normal

```bash
sqlmesh plan
sqlmesh run
```

## Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `host` | str | `localhost` | GizmoSQL server hostname |
| `port` | int | `31337` | GizmoSQL server port |
| `username` | str | *required* | Authentication username |
| `password` | str | *required* | Authentication password |
| `database` | str | `None` | Default database/catalog |
| `use_encryption` | bool | `True` | Use TLS encryption |
| `disable_certificate_verification` | bool | `False` | Skip TLS cert verification |
| `concurrent_tasks` | int | `4` | Max concurrent tasks |
| `register_comments` | bool | `True` | Register model comments |
| `pre_ping` | bool | `False` | Pre-ping connections |

## Features

- **Arrow Flight SQL**: Efficient data transfer using Arrow's columnar format
- **Full Catalog Support**: Create, drop, and switch between databases
- **Transaction Support**: Full transaction control via SQL statements
- **ADBC Bulk Ingestion**: Fast data loading using Arrow-native bulk operations
- **DuckDB Compatibility**: Uses DuckDB SQL dialect for query generation

## Requirements

- Python >= 3.10
- SQLMesh >= 0.100.0
- A running GizmoSQL server with DuckDB backend

## Development

```bash
# Clone the repository
git clone https://github.com/philip/sqlmesh-gizmosql.git
cd sqlmesh-gizmosql

# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run linting
ruff check .
mypy sqlmesh_gizmosql
```

## License

Apache 2.0
