import contextlib
import datetime
import sqlalchemy as sa
from pg_force_execute import pg_force_execute

# Run postgresql locally should allow the below to run
# ./start-services.sh

def test_basic():
    engine = sa.create_engine('postgresql://postgres@127.0.0.1:5432/')

    @contextlib.contextmanager
    def begin_ignore_terminated():
        try:
            with engine.begin() as conn:
                yield conn
        except sa.exc.OperationalError:
            pass

    with engine.begin() as conn:
        conn.execute(sa.text("DROP TABLE IF EXISTS pg_force_execute_test;"))
        conn.execute(sa.text("CREATE TABLE pg_force_execute_test(id int);"))

    with \
            begin_ignore_terminated() as conn_blocker, \
            engine.begin() as conn_blocked:

        conn_blocker.execute(sa.text("LOCK TABLE pg_force_execute_test IN ACCESS EXCLUSIVE MODE"))

        results = pg_force_execute(
            sa.text("SELECT * FROM pg_force_execute_test;"),
            conn_blocked,
            engine,
            delay=datetime.timedelta(seconds=1),
        ).fetchall()

    assert results == []
