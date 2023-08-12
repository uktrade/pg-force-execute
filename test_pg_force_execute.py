import contextlib
import datetime
import uuid
import pytest
import sqlalchemy as sa


# Both these cases are tested via CI
try:
    # psycopg3
    from psycopg import sql
    engine_type = 'postgresql+psycopg'
except ImportError:
    # psycopg2
    from psycopg2 import sql
    engine_type = 'postgresql+psycopg2'

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
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/')

    @contextlib.contextmanager
    def begin_ignore_terminated():
        try:
            with engine.begin() as conn:
                yield conn
        except sa.exc.OperationalError:
            pass

    with engine.begin() as conn:
        conn.execute(sa.text("DROP TABLE IF EXISTS pg_force_execute_test_1;"))
        conn.execute(sa.text("DROP TABLE IF EXISTS pg_force_execute_test_2;"))
        conn.execute(sa.text("CREATE TABLE pg_force_execute_test_1(id int);"))
        conn.execute(sa.text("CREATE TABLE pg_force_execute_test_2(id int);"))

    with \
            begin_ignore_terminated() as conn_blocker_1, \
            begin_ignore_terminated() as conn_blocker_2, \
            engine.begin() as conn_blocked, \
            pg_force_execute(conn_blocked, delay=delay):

        conn_blocker_1.execute(sa.text("LOCK TABLE pg_force_execute_test_1 IN ACCESS EXCLUSIVE MODE"))
        conn_blocker_2.execute(sa.text("LOCK TABLE pg_force_execute_test_2 IN ACCESS EXCLUSIVE MODE"))

        start = datetime.datetime.now()
        results = conn_blocked.execute(sa.text("SELECT * FROM pg_force_execute_test_1;")).fetchall()
        results += conn_blocked.execute(sa.text("SELECT * FROM pg_force_execute_test_2;")).fetchall()
        end = datetime.datetime.now()

    assert results == []
    assert end - start >= delay
    assert end - start < delay + datetime.timedelta(seconds=2)


def test_non_blocking():
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/')

    with \
            engine.begin() as conn, \
            pg_force_execute(conn, delay=datetime.timedelta(seconds=5)):

        start = datetime.datetime.now()
        results = conn.execute(sa.text("SELECT 1")).fetchall()
        end = datetime.datetime.now()

    assert results == [(1,)]
    assert end - start < datetime.timedelta(seconds=1)


def test_cancel_exception_propagates():
    user = uuid.uuid4().hex
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/')
    bad_engine = sa.create_engine(f'{engine_type}://{user}:password@127.0.0.1:5432/postgres')

    @contextlib.contextmanager
    def begin_ignore_terminated(engine):
        try:
            with engine.begin() as conn:
                yield conn
        except sa.exc.OperationalError:
            pass

    with engine.begin() as conn:
        driver_connection = conn.connection.driver_connection
        conn.execute(sa.text("DROP TABLE IF EXISTS pg_force_execute_test;"))
        conn.execute(sa.text("CREATE TABLE pg_force_execute_test(id int);"))
        conn.execute(sa.text(sql.SQL("CREATE USER {} PASSWORD 'password';").format(sql.Identifier(user)).as_string(driver_connection)))
        conn.execute(sa.text(sql.SQL("GRANT SELECT ON pg_force_execute_test TO {}").format(sql.Identifier(user)).as_string(driver_connection)))

    with \
            pytest.raises(sa.exc.ProgrammingError, match='must be a superuser|Only roles with the SUPERUSER attribute'), \
            begin_ignore_terminated(engine) as conn_blocker_1, \
            bad_engine.begin() as conn_blocked, \
            pg_force_execute(conn_blocked, delay=datetime.timedelta(seconds=1)):

        conn_blocker_1.execute(sa.text("LOCK TABLE pg_force_execute_test_1 IN ACCESS EXCLUSIVE MODE"))
        conn_blocked.execute(sa.text("SET statement_timeout=2000"))
        conn_blocked.execute(sa.text("SELECT * FROM pg_force_execute_test_1;")).fetchall()


def test_query_exception_propagates():
    engine = sa.create_engine(f'{engine_type}://postgres@127.0.0.1:5432/')

    with \
            pytest.raises(sa.exc.ProgrammingError, match='table_does_not_exist'), \
            engine.begin() as conn, \
            pg_force_execute(conn, delay=datetime.timedelta(seconds=1)):

        conn.execute(sa.text("SELECT * FROM table_does_not_exist;"))
