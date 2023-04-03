import datetime
import sqlalchemy as sa
from pg_force_execute import pg_force_execute

# Run postgresql locally should allow the below to run
# docker run --rm -it -e POSTGRES_HOST_AUTH_METHOD=trust -p 5432:5432 postgres

engine = sa.create_engine('postgresql://postgres@127.0.0.1:5432/')

with engine.begin() as conn:
    results = pg_force_execute(
        sa.text('SELECT 1'),
        conn,
        engine,
        delay=datetime.timedelta(minutes=5),
    )
    print(results.fetchall())
