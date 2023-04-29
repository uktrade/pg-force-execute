import contextlib
import datetime
import pytest
import sqlalchemy as sa
from pg_force_execute import pg_force_execute

# Run postgresql locally should allow the below to run
# ./start-services.sh

@pytest.mark.parametrize(
    "delay",
    (
        datetime.timedelta(seconds=1), 
        datetime.timedelta(seconds=5),
    )
)
def test_blocking(delay):
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

        start = datetime.datetime.now()
        results = pg_force_execute(
            sa.text("SELECT * FROM pg_force_execute_test;"),
            conn_blocked,
            engine,
            delay=delay,
        ).fetchall()
        end = datetime.datetime.now()

    assert results == []
    assert end - start >= delay
    assert end - start < delay + datetime.timedelta(seconds=2)


def test_non_blocking():
    engine = sa.create_engine('postgresql://postgres@127.0.0.1:5432/')

    with engine.begin() as conn:
        start = datetime.datetime.now()
        results = pg_force_execute(
            sa.text("SELECT 1"),
            conn,
            engine,
            delay=datetime.timedelta(seconds=5),
        ).fetchall()
        end = datetime.datetime.now()

    assert results == [(1,)]
    assert end - start < datetime.timedelta(seconds=1)


def test_cancel_exception_propagates():
    engine = sa.create_engine('postgresql://postgres@127.0.0.1:5432/')
    bad_engine = sa.create_engine('postgresql://user_does_not_exist@127.0.0.1:5432/')

    with engine.begin() as conn:
        conn.execute(sa.text("DROP TABLE IF EXISTS pg_force_execute_test;"))
        conn.execute(sa.text("CREATE TABLE pg_force_execute_test(id int);"))

    with \
                pytest.raises(sa.exc.OperationalError, match='user_does_not_exist'), \
                engine.begin() as conn_blocked:

            pg_force_execute(
                sa.text("SELECT pg_sleep(3);"),
                conn_blocked,
                bad_engine,
                delay=datetime.timedelta(seconds=1),
            ).fetchall()


def test_query_exception_propagates():
    engine = sa.create_engine('postgresql://postgres@127.0.0.1:5432/')

    with \
                pytest.raises(sa.exc.ProgrammingError, match='table_does_not_exist'), \
                engine.begin() as conn:

            pg_force_execute(
                sa.text("SELECT * FROM table_does_not_exist;"),
                conn,
                engine,
                delay=datetime.timedelta(seconds=1),
            )
