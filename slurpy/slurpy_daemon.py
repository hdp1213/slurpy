import slurpy

import logging
from logging import handlers
from apscheduler.schedulers.blocking import BlockingScheduler

from datetime import datetime
from os.path import join as path_join
from time import sleep, clock

# "Configurable" constants
LOG_FORMAT = '[{asctime}] {name:<18}: {levelname:<8} {message}'
LOG_ROOT = '/home/a1648400/.local/log'
LOG_SIZE_BYTES = 100000
LOG_BACKUPS = 3

SLEEP_TIME = 1

TS_FORMAT = '%Y%m%d_%H%M%S'
NODE_ROOT = '/fast/users/a1648400/slurpyd'

# "Actual" constants
MAIN_LOG = 'slurpyd'
NODE_LOG = 'slurpyd.NodeTrack'
JOBS_LOG = 'slurpyd.JobOrders'

R_NODES = r'r[1-4]n[0-9]{2}'


def main():
    setup_loggers()

    mlog = get_slurpyd_logger(MAIN_LOG)
    nlog = get_slurpyd_logger(NODE_LOG)

    mlog.info("Starting slurpyd ...")

    scheduler = BlockingScheduler(timezone='Australia/Adelaide')

    scheduler.add_job(write_nodes, 'cron',
                      second='*/{}'.format(SLEEP_TIME),
                      max_instances=2)

    scheduler.start()


def write_nodes():
    nlog = get_slurpyd_logger(NODE_LOG)

    nlog.info("Querying SLURM nodes")
    ts = datetime.now().strftime(TS_FORMAT)

    node_time = clock()
    raw_df = slurpy.get_node_df()
    node_time = clock() - node_time

    nlog.debug("Querying took {:.4} s", node_time)

    node_time = clock()
    rnodes_df = slurpy.filter_df(raw_df,
                                 column='NodeName',
                                 patterns=[R_NODES])
    node_time = clock() - node_time

    nlog.debug("Filtering took {:.4} s", node_time)

    rnodes_filename = 'rnodes-{}.gz'.format(ts)
    rnodes_path = path_join(NODE_ROOT, rnodes_filename)

    nlog.info("Writing nodes to {}", rnodes_filename)
    rnodes_df.to_pickle(rnodes_path)


def setup_loggers():
    logging._srcfile = None
    logging.logThreads = 0
    logging.logProcesses = 0

    log_filename = path_join(LOG_ROOT, 'slurpyd.log')

    slurpy_logger = logging.getLogger(MAIN_LOG)
    slurpy_logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(fmt=LOG_FORMAT,
                                  style='{')

    rfh = handlers.RotatingFileHandler(filename=log_filename, mode='w',
                                       maxBytes=LOG_SIZE_BYTES,
                                       backupCount=LOG_BACKUPS)
    rfh.setLevel(logging.DEBUG)
    rfh.setFormatter(formatter)
    slurpy_logger.addHandler(rfh)

    # sh = logging.StreamHandler()
    # sh.setLevel(logging.INFO)
    # sh.setFormatter(formatter)
    # slurpy_logger.addHandler(sh)

    slurpy_logger.info("Writing logs to %s", log_filename)


def get_slurpyd_logger(logger_name):
    return FormatAdapter(logging.getLogger(logger_name))


class FormatAdapter(logging.LoggerAdapter):
    def __init__(self, logger, extra=None):
        super(FormatAdapter, self).__init__(logger, extra or {})

    def log(self, level, msg, *args, **kwargs):
        if self.isEnabledFor(level):
            msg, kwargs = self.process(msg, kwargs)
            self.logger._log(level, msg.format(*args, **kwargs), (), {})


if __name__ == '__main__':
    exit(main())
