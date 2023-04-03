import datetime
import logging
import threading

from psycopg2 import sql

logger = logging.getLogger(__name__)


def pg_force_execute(conn, engine, query):

    def force_unblock(pid, exit):
        started = datetime.datetime.utcnow()

        while not exit.is_set():
            if datetime.datetime.utcnow() - started > timedelta(minutes=5):
                break
            sleep(5)

        while not exit.is_set():
            with engine.begin() as conn:
                # pg_cancel_backend isn't strong enough - the blocking PIDs might not be
                # actually running a query, so there is nothing to cancel. They might
                # just be holding locks. To force release of the locks, we have to call
                # pg_terminate_backend
                cancelled_queries = conn.execute(
                    sql.SQL(
                        """
                    SELECT
                        activity.usename AS usename,
                        activity.query AS query,
                        age(clock_timestamp(), activity.query_start) AS age,
                        pg_terminate_backend(pids.pid)
                    FROM
                        UNNEST(pg_blocking_pids({})) AS pids(pid)
                    INNER JOIN
                        pg_stat_activity activity ON activity.pid = pids.pid;
                    """
                    )
                    .format(sql.Literal(pid))
                    .as_string(conn.connection.cursor())
                ).fetchall()
                logger.info("# queries to cancel: %s", len(cancelled_queries))
                for cancelled_query in cancelled_queries:
                    logger.exception('Cancelled query %s', cancelled_query)
            sleep(5)

    pid = conn.execute(
        sql.SQL('SELECT pg_backend_pid()')
        .format()
        .as_string(conn.connection.cursor())
    ).fetchall()[0][0]
    exit = Event()
    t = Thread(target=force_unblock, args=(pid, exit))
    t.start()

    try:
        return conn.execute(query)
    finally:
        exit.set()
        t.join(timeout=10)
