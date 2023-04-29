# pg-force-execute

Utility function to run a PostgreSQL query with SQLAlchemy, terminating any other clients that continue to block it after a configurable delay.

Using this function to run queries is somewhat of a last resort, but is useful in certain Extract Transform Load (ETL) pipeline contexts. For example, if it is more important to replace one table with another than to allow running queries on the table to complete, then this function can be used to run the relevant `ALTER TABLE RENAME TO` query.


## Installation

```bash
pip install pg-force-execute
```


## Example usage

```python
import datetime
import sqlalchemy as sa
from pg_force_execute import pg_force_execute

# Run postgresql locally should allow the below to run
# docker run --rm -it -e POSTGRES_HOST_AUTH_METHOD=trust -p 5432:5432 postgres

engine = sa.create_engine('postgresql://postgres@127.0.0.1:5432/')
query = 'SELECT 1'  # A more realistic example would be something that needs an exclusive lock on a table

with engine.begin() as conn:
    results = pg_force_execute(
        sa.text(query), # SQLAlchemy statement to execute
        conn,           # SQLAlchemy connection to run the query
        engine,         # SQLAlchemy engine that will create new connections to cancel blocking queries
        delay=datetime.timedelta(minutes=5),  # Amount of time to wait before cancelling queries
    )
    print(results.fetchall())
```


## API

The API a single function `pg_force_execute`.

`pg_force_execute`(statement, conn, engine, parameters=None, execution_options=None, delay=datetime.timedelta(minutes=5), check_interval=datetime.timedelta(seconds=1), termination_thread_timeout=datetime.timedelta(seconds=10), logger=logging.getLogger("pg_force_execute"))

- `statement` - A SQLAlchemy statement to be executed, passed to [Connection.execute](https://docs.sqlalchemy.org/en/20/core/connections.html#sqlalchemy.engine.Connection.execute.params.statement)

- `conn` - A [SQLAlchemy connection](https://docs.sqlalchemy.org/en/20/core/connections.html#sqlalchemy.engine.Connection) to run `statement` on

- `engine` - A [SQLAlchemy engine](https://docs.sqlalchemy.org/en/20/core/connections.html#sqlalchemy.engine.Engine) to create a new connection that will be used to terminate backends blocking `statement`

- `parameters` (optional) - SQLAlchemy parameters to be bound to `statement`, passed to [Connection.execute](https://docs.sqlalchemy.org/en/20/core/connections.html#sqlalchemy.engine.Connection.execute.params.parameters)

- `execution_options` (optional) - Dictionary of execution options assocated with the `statement` execution, passed to [Connection.execute](https://docs.sqlalchemy.org/en/20/core/connections.html#sqlalchemy.engine.Connection.execute.params.execution_options)

- `delay` (optional) - How long to wait before attempting to terminate backends blocking `statement`

- `check_interval` (optional) - The interval between repeated attempted to terminate backends blocking `statement`

- `termination_thread_timeout` (optional) - How long to wait for the termination to complete

- `logger` (optional) The Python logger instance through which to log


## Running tests locally

```bash
pip install -e ".[dev]"  # Only needed once
./start-services.sh      # Only needed once
pytest
```
