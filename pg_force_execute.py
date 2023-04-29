import datetime
import logging
from threading import Event, Thread

import sqlalchemy as sa


def pg_force_execute(query, conn, engine,
                     delay=datetime.timedelta(minutes=5),
                     check_interval=datetime.timedelta(seconds=1),
                     cancel_timeout=datetime.timedelta(seconds=10),
                     logger=logging.getLogger("pg_force_execute"),
):

    def force_unblock(pid, exit):
        exit.wait(timeout=delay.total_seconds())

        # Repeatedly check for other backends that block `query`, and cancel them if
        # they are. The repeat check is to avoid race conditions - if another backend
        # blocks this backend just after pg_blocking_pids returns its PIDs. If it's
        # determined that PostgreSQL forbids this case the looping can be removed
        while not exit.wait(timeout=check_interval.total_seconds()):
            logger.info('Cancelling queries blocking PID %s running %s', pid, query)
            with engine.begin() as conn:
                # pg_cancel_backend isn't strong enough - the blocking PIDs might not be
                # actually running a query, so there is nothing to cancel. They might
                # just be holding locks. To force release of the locks, we have to call
                # pg_terminate_backend
                cancelled_queries = conn.execute(
                    sa.text("""
                        SELECT
                            activity.usename AS usename,
                            activity.query AS query,
                            age(clock_timestamp(), activity.query_start) AS age,
                            pg_terminate_backend(pids.pid)
                        FROM
                            UNNEST(pg_blocking_pids(:pid)) AS pids(pid)
                        INNER JOIN
                            pg_stat_activity activity ON activity.pid = pids.pid;
                    """), {"pid": pid},
                ).fetchall()
                if not cancelled_queries:
                    logger.info('No blocking queries to cancel')
                for cancelled_query in cancelled_queries:
                    logger.error('Cancelled blocking query %s', cancelled_query)

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
        t.join(timeout=cancel_timeout.total_seconds())
