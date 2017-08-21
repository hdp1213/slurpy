import slurpy

import logging
from logging.handlers import RotatingFileHandler
from apscheduler.schedulers.blocking import BlockingScheduler

import argparse
from configparser import ConfigParser, ExtendedInterpolation

from datetime import datetime
from os.path import join as path_join, expanduser
from time import clock

CONFIG_ROOT = '~/.config/slurpyd.ini'

MAIN_LOG = 'slurpyd'
NODE_LOG = 'slurpyd.NodeTrack'
JOBS_LOG = 'slurpyd.JobOrders'

R_NODES = r'r[1-4]n[0-9]{2}'

VERBOSE_LEVEL = {1: logging.INFO,
                 2: logging.DEBUG}


def main():
    parser = make_parser()
    args = parser.parse_args()

    config = ConfigParser(interpolation=ExtendedInterpolation())

    with open(expanduser(args.config_file), mode='r') as conf:
        config.read_file(conf)

    log_config = config['log']
    node_config = config['node query']

    setup_loggers(args, log_config)

    mlog = get_slurpyd_logger(MAIN_LOG)
    nlog = get_slurpyd_logger(NODE_LOG)

    mlog.info("Starting slurpyd ...")

    scheduler = BlockingScheduler(timezone='Australia/Adelaide')

    nlog.info("Saving nodes to directory {}", node_config['output'])

    scheduler.add_job(write_nodes, 'cron',
                      max_instances=2,
                      **get_cron_freq(node_config),
                      args=[node_config])

    scheduler.start()


def write_nodes(node_config):
    nlog = get_slurpyd_logger(NODE_LOG)

    nlog.info("Querying SLURM nodes")
    node_filename = datetime.now().strftime(node_config['filename'])

    node_time = clock()
    raw_node_df = slurpy.get_node_df()
    node_time = clock() - node_time

    nlog.debug("Querying took {:.4} s", node_time)

    node_time = clock()
    rnode_df = slurpy.filter_df(raw_node_df,
                                column='NodeName',
                                patterns=[R_NODES])
    node_time = clock() - node_time

    nlog.debug("Filtering took {:.4} s", node_time)

    rnodes_path = path_join(node_config['output'], node_filename)

    nlog.info("Saving node state as {}", node_filename)
    try:
        rnode_df.to_pickle(rnodes_path)
    except FileNotFoundError:
        nlog.exception("Saving to {} failed:", node_config['output'])


def setup_loggers(args, log_config):
    # logging settings
    logging._srcfile = None
    logging.logThreads = 0
    logging.logProcesses = 0

    if args.log_file is None:
        log_filename = path_join(log_config['output'], 'slurpyd.log')
    else:
        log_filename = path_join(args.log_file, 'slurpyd.log')

    slurpy_logger = logging.getLogger(MAIN_LOG)
    slurpy_logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(fmt=log_config['format'], style='{')

    rfh = RotatingFileHandler(filename=log_filename, mode='w',
                              maxBytes=log_config.getint('max bytes'),
                              backupCount=log_config.getint('backups'))
    rfh.setLevel(logging.DEBUG)
    rfh.setFormatter(formatter)
    slurpy_logger.addHandler(rfh)

    if args.verbosity > 0:
        sh = handlers.StreamHandler()
        sh.setLevel(VERBOSE_LEVEL.get(args.verbosity, logging.DEBUG))
        sh.setFormatter(formatter)
        slurpy_logger.addHandler(sh)

    slurpy_logger.info("Writing logs to %s", log_filename)


def get_slurpyd_logger(logger_name):
    return FormatAdapter(logging.getLogger(logger_name))


def get_cron_freq(opt_config):
    units = opt_config['units']
    cron = '*/{}'.format(opt_config['frequency'])
    return {units: cron}


def make_parser():
    parser = argparse.ArgumentParser(description='%(prog)s, a slurpy daemon.')

    parser.add_argument('--config',
                        type=str,
                        dest='config_file',
                        default=CONFIG_ROOT,
                        help='specify external config file '
                             '(default: %(default)s)')

    parser.add_argument('--log',
                        type=str,
                        dest='log_file',
                        help='specify log file location')

    parser.add_argument('-v', '--verbose',
                        action='count',
                        default=0,
                        dest='verbosity',
                        help='specify level of verbosity')

    parser.add_argument('--version',
                        action='version',
                        version='%(prog)s v0.1')

    return parser


class FormatAdapter(logging.LoggerAdapter):
    def __init__(self, logger, extra=None):
        super(FormatAdapter, self).__init__(logger, extra or {})

    def log(self, level, msg, *args, **kwargs):
        if self.isEnabledFor(level):
            msg, kwargs = self.process(msg, kwargs)
            self.logger._log(level, msg.format(*args, **kwargs),
                             (), **kwargs)


if __name__ == '__main__':
    exit(main())
