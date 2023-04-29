import datetime
import logging
from threading import Event, Thread
from time import sleep

import sqlalchemy as sa
logger = logging.getLogger(__name__)


def pg_force_execute(query, conn, engine, delay=datetime.timedelta(minutes=5)):

    def force_unblock(pid, exit):
        started = datetime.datetime.utcnow()

        while not exit.is_set():
            if datetime.datetime.utcnow() - started > delay:
                break
            sleep(1)

        while not exit.is_set():
            with engine.begin() as conn:
                # pg_cancel_backend isn't strong enough - the blocking PIDs might not be
                # actually running a query, so there is nothing to cancel. They might
                # just be holding locks. To force release of the locks, we have to call
                # pg_terminate_backend
                cancelled_queries = conn.execute(
                    sa.text(
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
                    """, pid)
                ).fetchall()
                logger.info("# queries to cancel: %s", len(cancelled_queries))
                for cancelled_query in cancelled_queries:
                    logger.error('Cancelled query %s', cancelled_query)
            sleep(1)

    pid = conn.execute(
        sa.text('SELECT pg_backend_pid()')
    ).fetchall()[0][0]
    exit = Event()
    t = Thread(target=force_unblock, args=(pid, exit))
    t.start()

    try:
        return conn.execute(query)
    finally:
        exit.set()
        t.join(timeout=10)
