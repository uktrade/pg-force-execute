# pg-force-execute

Utility function to run a PostgreSQL query with SQLAlchemy, terminating any queries that continue to block it after a configurable delay.


## Installation

```bash
pip install pg-force-execute
```


## Usage

```python
import datetime
import sqlalchemy as sa
from pg_force_execute import pg_force_execute

results = pg_force_execute(
    sa.text(query), # SQL query to execute
    conn,           # SQLAlchemy connection to run the query
    engine,         # SQLAlchemy engine that will create new connections to cancel blocking queries
    delay=datetime.timedelta(minutes=5),  # Amount of time to wait before cancelling queries
)
print(results.fetchall())
```
