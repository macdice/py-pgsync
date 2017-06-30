# PgSyncPool
#
# This is proof-of-concept throw-away code for experimentation only!  It
# demonstrates the sort of control flow that might be required to make use of
# the proposed synchronous_replay feature of PostgreSQL:
#
# https://www.postgresql.org/message-id/flat/CAEepm%3D1iiEzCVLD%3DRoBgtZSyEY1CR-Et7fRc9prCZ9MuTz3pWg%40mail.gmail.com
#
# Runs against a patched PostgreSQL installation.  The hard-coded connection
# strings assume a set of 4 database servers started with
# test-synchronous-replay.sh (from the above thread).  To test state
# transition behaviour, try pausing replay on a standby.
#
# The basic conditions required to make this sort of scheme work seem to be:
#
#  1.  Units of work run in framework-managed transactions
#  2.  Framework can retry whole units of work automatically
#  3.  Units of work can be declared to be read-only
#
# Spring, J2EE and Django seem to fit the bill, where "units of work"
# correspond to handlers for requests from clients, but I haven't yet explored
# the details and my Java is extremely rusty...  So I'm going to present this
# working pseudo-code to some Java application gurus I know and see what they
# have to say about it!
#
# This code doesn't handle pool size management, errors, or much else, and
# it never will because the point isn't to do that, it's just to show that
# appropriate transaction routing and retry should be feasible with a real
# application server environment.

import psycopg2
import random
import threading
import time

class PgSyncPool:
    """A connection pool that can manage connections to a primary database
       server and some number of replicas, keeping track of which ones are
       currently available for 'synchronous replay'."""

    class Connection:
        """We need to track which pool a connection came from, so we'll
           do that with a wrapper class.  There's probably a better way."""
        def __init__(self, connection, pool):
            self.connection = connection
            self.pool = pool

    class Pool:
        def __init__(self, url):
            self.url = url
            self.connections = []
            self.busy = 0
            self.back_off_until = None

        def get_connection(self):
            """Find or create a connection."""
            if len(self.connections) > 0:
                connection = self.connections.pop()
            else:
                connection = PgSyncPool.Connection(psycopg2.connect(self.url), self)
                cursor = connection.connection.cursor()
                cursor.execute("SET synchronous_replay = on")
            self.busy = self.busy + 1
            return connection

        def put_connection(self, connection, abandon):
            """Return a connection to the pool, or abandon it."""
            self.busy = self.busy - 1
            if abandon:
                try:
                    connection.connection.close()
                except:
                    pass
            else:
                self.connections.append(connection)

    def __init__(self, primary_url, replica_urls):
        self.primary_pool = self.Pool(primary_url)
        self.replica_pools = [self.Pool(url) for url in replica_urls]
        self.lock = threading.Lock()

    def choose_read_pool(self):
        """Decide which read-only pool to use.  Various policies would be
           possible, but for now: choose a random server among those with
           the fewest busy connections, or None if none suitable."""

        # find all pools that don't have a future back-off time
        pools = []
        now = None
        for pool in self.replica_pools:
            if pool.back_off_until != None:
                # avoid syscall until first needed
                if now == None:
                    now = time.time()
                if pool.back_off_until < now:
                    pool.back_off_until = None
            if pool.back_off_until == None:
                pools.append(pool)

        # pick a random one among those with the lowest busy connection count
        random.shuffle(pools)
        least_busy = None
        for pool in pools:
            if least_busy == None or pool.busy < least_busy.busy:
                least_busy = pool

        return least_busy

    def get_connection(self, read_only):
        """Get a database connection.  Try to use a connection to a read-only
           replica server one if read_only is True, but fall back to a primary
           connection if necessary."""
        with self.lock:
            pool = None
            if read_only:
                pool = self.choose_read_pool()
            if pool == None:
                pool = self.primary_pool
            return pool.get_connection()

    def put_connection(self, connection, abandon):
        """Return a connection to the pool that owns it."""
        with self.lock:
            connection.pool.put_connection(connection, abandon)

    def back_off(self, connection, seconds):
        """Record that we should avoid using the server that this connection
           came from for a period of time, because it's known not to support
           synchronous_replay at the moment."""
        connection.pool.back_off_until = time.time() + seconds

if __name__ == "__main__":
    # The rest of this code is a test harness to drive the pool in an
    # interesting way.  The idea is to run a mix of read and write
    # transactions with the right kind of retry logic to handle standbys
    # being kicked out of the synchronous replay set.

    class StrawManAppServer:
        """A mechanism that can run transactional application code.  This
           is standing in for the request handler machinery of a real
           application server environment."""

        def __init__(self, syncpool, max_retries, back_off_time):
            self.syncpool = syncpool
            self.max_retries = max_retries
            self.back_off_time = back_off_time

        def run_transaction(self, handler, read_only):
            """Stand in for the handler dispatch mechanism of a real
               application server."""

            for retry in range(self.max_retries):
                connection = self.syncpool.get_connection(read_only)
                abandon = True
                try:
                    # run the caller's code, and commit if we get that far
                    handler(connection.connection)
                    connection.connection.commit()
                    abandon = False
                    return True
                except psycopg2.Error, e:
                    if e.pgcode == "40P02":
                        # synchronous replay not available; don't talk to
                        # this server for a while, and retry transaction
                        # somewhere else; it's ok to reuse the connection
                        # though
                        print "will reroute because synchronous replay not currently available"
                        self.syncpool.back_off(connection, self.back_off_time)
                        connection.connection.rollback()
                        abandon = False
                        # we'll retry...
                    # handle other retryable things like serialization
                    # failure, deadlock, too
                    else:
                        # otherwise can only reuse connection if we can roll
                        # back the transaction
                        try:
                           connection.connection.rollback()
                           abandon = False
                        except:
                           print "failed to roll back transaction; abandoning connection"
                           pass
                        raise e
                finally:
                    # return (or abandon) the connection
                    self.syncpool.put_connection(connection, abandon)

            print "max retries exceeded"
            return False

    # Configuration
    WRITE_URL = "postgresql://:5432/postgres"
    READ_URLS = ["postgresql://:5441/postgres",
                 "postgresql://:5442/postgres",
                 "postgresql://:5443/postgres"]
    MAX_RETRIES = 5
    BACK_OFF_TIME = 10

    num_threads = 10  # number of concurrent threads
    loops = 10000     # how many transactions each thread should run
    pct_reads = 90    # what mix of reads vs writes

    pool = PgSyncPool(WRITE_URL, READ_URLS)
    server = StrawManAppServer(pool, MAX_RETRIES, BACK_OFF_TIME)

    def my_setup_transaction(connection):
        """Set up a database object."""
        cursor = connection.cursor()
        cursor.execute("DROP TABLE IF EXISTS widgets")
        cursor.execute("CREATE TABLE widgets (a int PRIMARY KEY, b int)")
        for i in range(1000):
            cursor.execute("INSERT INTO widgets (a, b) VALUES (%s, 0)", [i])
        cursor.close()

    def my_write_transaction(connection):
        """Update some random rows."""
        ids = [random.randint(0, 1000) for i in range(5)]
        ids.sort() # avoid deadlocks
        cursor = connection.cursor()
        for id in ids:
            cursor.execute("UPDATE widgets SET b = b + 1 WHERE a = %s", [id])
        cursor.close()

    def my_read_transaction(connection):
        """Do some utterly pointless read-only work."""
        cursor = connection.cursor()
        cursor.execute("SELECT sum(b) FROM widgets WHERE a % 2 = 0")
        cursor.execute("SELECT sum(b) FROM widgets WHERE a % 2 = 1")
        cursor.close()

    def my_client_thread_main(loops, pct_reads):
        for n in range(loops):
            if random.randint(0, 100) <= pct_reads:
                server.run_transaction(my_read_transaction, True)
            else:
                server.run_transaction(my_write_transaction, False)

    server.run_transaction(my_setup_transaction, False)

    # start threads
    threads = []
    for n in range(num_threads):
        thread = threading.Thread(target = my_client_thread_main, args = [loops, pct_reads])
        threads.append(thread)
        thread.start()

    # join threads
    for thread in threads:
        thread.join()
