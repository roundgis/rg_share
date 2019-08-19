#manage redis

from twisted.internet import protocol, defer, error, threads
from twisted.internet import reactor
import sys
import os.path as opath
import os
import sqlite3


DB_FILE = '/home/pi/rg_share.db'

REDIS_PATH = '/home/pi/redis-server'

REDIS_CFG = '/home/pi/redis_mem.conf'

REDIS_LISTEN_PORT = 21999

REDIS_MAX_MEM = 32 #mb

SYS_CFG_TBL = 'sys_cfg'

LOG_PATH = '/home/pi/rglog'


CREATE_SYS_CFG_TBL = """create table if not exists {0} (key text not null primary key,
                                                            val text not null) """.format(SYS_CFG_TBL)


class ConnWrap:
    def __init__(self, conn_obj):
        self.conn_obj = conn_obj

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None and exc_val is None and exc_tb is None:
            self.conn_obj.commit()
        else:
            self.conn_obj.rollback()
        self.conn_obj.close()


def CreateDB():
    with ConnWrap(sqlite3.connect(DB_FILE, check_same_thread=False)) as conn:
        conn.conn_obj.execute("PRAGMA journal_mode=WAL")
        conn.conn_obj.execute("BEGIN")
        conn.conn_obj.execute(CREATE_SYS_CFG_TBL)


def CreateLogPath():
    if not opath.isdir(LOG_PATH):
        os.makedirs(LOG_PATH)


def CreateRedisCfg():
    lines = [
        'port {0}'.format(REDIS_LISTEN_PORT),
        'tcp-backlog {0}'.format(128),
        'timeout {0}'.format(0),
        'tcp-keepalive {0}'.format(0),
        'logfile "{0}"'.format(''),
        'databases {0}'.format(16),
        'stop-writes-on-bgsave-error {0}'.format('yes'),
        'maxclients {0}'.format(10000),
        'maxmemory {0}mb'.format(REDIS_MAX_MEM),
        'maxmemory-policy {0}'.format('volatile-lru'),
        'appendonly {0}'.format('no'),
        'appendfsync {0}'.format('everysec'),
        'no-appendfsync-on-rewrite {0}'.format('no'),
        'auto-aof-rewrite-percentage {0}'.format(100),
        'auto-aof-rewrite-min-size {0}mb'.format(64),
        'aof-load-truncated {0}'.format('yes'),
        'lua-time-limit {0}'.format(5000),
        'slowlog-log-slower-than {0}'.format(10000),
        'slowlog-max-len {0}'.format(128),
        'latency-monitor-threshold {0}'.format(0),
        'hash-max-ziplist-entries {0}'.format(512),
        'hash-max-ziplist-value {0}'.format(64),
        'list-max-ziplist-entries {0}'.format(512),
        'list-max-ziplist-value {0}'.format(64),
        'set-max-intset-entries {0}'.format(512),
        'zset-max-ziplist-entries {0}'.format(128),
        'zset-max-ziplist-value {0}'.format(64),
        'hll-sparse-max-bytes {0}'.format(3000),
        'activerehashing {0}'.format('yes'),
        'client-output-buffer-limit normal {0} {1} {2}'.format(0, 0, 0),
        'client-output-buffer-limit slave {0}mb {1}mb {2}'.format(256, 64, 60),
        'client-output-buffer-limit pubsub {0}mb {1}mb {2}'.format(32, 8, 60),
        'hz {0}'.format(10),
        'aof-rewrite-incremental-fsync {0}'.format('yes')
    ]
    with open(REDIS_CFG, 'wb') as f:
        for line in lines:
            f.write(line.encode('utf-8'))
            f.write(os.linesep.encode('utf-8'))


class RGProcess(protocol.ProcessProtocol):
    def __init__(self, text):
        self.text = text
        self.defer_obj = defer.Deferred()

    def connectionMade(self):
        self.transport.closeStdin()

    def outReceived(self, data):
        pass

    def inReceived(self, data):
        pass

    def processEnded(self, reason):
        self.defer_obj.callback(reason.value)


def StartRedis():
    tp = RGProcess('')
    CreateRedisCfg()
    reactor.spawnProcess(tp, REDIS_PATH, [REDIS_PATH, REDIS_CFG], {})


@defer.inlineCallbacks
def SyncTime():
    hwclock_path = '/sbin/hwclock'
    if opath.isfile(hwclock_path):
        tp = RGProcess('sync time from hw clock')
        reactor.spawnProcess(tp, hwclock_path, [hwclock_path, "-s"], {})
        yield tp.defer_obj


@defer.inlineCallbacks
def ExpandRootFS():
    @defer.inlineCallbacks
    def __Expand():
        raspi_config = '/usr/bin/raspi-config'
        if opath.isfile(raspi_config):
            tp = RGProcess('expand root fs')
            reactor.spawnProcess(tp, raspi_config, [raspi_config, "--expand-rootfs"], {})
            val = yield tp.defer_obj
            if isinstance(val, error.ProcessDone):
                defer.returnValue(True)
            elif isinstance(val, error.ProcessTerminated):
                defer.returnValue(False)
            else:
                defer.returnValue(False)
        else:
            defer.returnValue(False)

    def __CheckDB():
        with ConnWrap(sqlite3.connect(DB_FILE, check_same_thread=False)) as conn:
            rows = list(conn.conn_obj.execute("select * from {0} where key=?".format(SYS_CFG_TBL),
                                              ("has_expand_fs", )))
            if len(rows) > 0:
                return rows[0][1] == 'yes'
            else:
                return False

    def __UpdateDB():
        with ConnWrap(sqlite3.connect(DB_FILE, check_same_thread=False)) as conn:
            sql1 = "insert or ignore into {0} values(?,?)"
            sql2 = "update or ignore {0} set val=? where key=?"
            conn.conn_obj.execute("BEGIN")
            conn.conn_obj.execute(sql1.format(SYS_CFG_TBL),
                                  ("has_expand_fs", "yes"))
            conn.conn_obj.execute(sql2.format(SYS_CFG_TBL),
                                  ("yes", "has_expand_fs"))
            return True

    has_expanded = yield threads.deferToThread(__CheckDB)
    if not has_expanded:
        val = yield __Expand()
        if val:
            yield threads.deferToThread(__UpdateDB)


@defer.inlineCallbacks
def StartAllProcess():
    yield SyncTime()
    yield ExpandRootFS()
    yield defer.maybeDeferred(StartRedis)


if __name__ == "__main__":
    CreateDB()
    CreateLogPath()
    reactor.callLater(0, StartAllProcess)
    reactor.run()
