import slurpy

import logging
from logging.handlers import RotatingFileHandler
from apscheduler.schedulers.blocking import BlockingScheduler

import argparse
from configparser import ConfigParser, ExtendedInterpolation

from datetime import datetime
from os.path import join as path_join, expanduser, basename
from time import perf_counter as tick

CONFIG_ROOT = '~/.config/slurpyd.ini'

MAIN_LOG = 'slurpyd'
NODE_LOG = 'slurpyd.NodeTrack'
MRGE_LOG = 'slurpyd.NodeMerge'
JOBS_LOG = 'slurpyd.JobOrders'

VERBOSE_LEVEL = {1: logging.INFO,
                 2: logging.DEBUG}

STREAM_FMT = '{name:<18}: {levelname:<8} {message}'

S_TO_MS = 1000

INVALID_FEATURE = 20
INVALID_PROPERTY = 21
INVALID_FORMAT = 30


def main():
    parser = make_parser()
    args = parser.parse_args()

    config = ConfigParser(interpolation=ExtendedInterpolation())

    with open(expanduser(args.config_file), mode='r') as conf:
        config.read_file(conf)

    log_config = config['Log']
    node_config = config['NodeTrack']
    job_config = config['JobOrders']

    setup_loggers(args, log_config)

    mlog = get_slurpyd_logger(MAIN_LOG)
    nlog = get_slurpyd_logger(NODE_LOG)
    jlog = get_slurpyd_logger(JOBS_LOG)

    mlog.info("Starting slurpyd ...")

    try:
        slurpy.check_node_features(node_config['features'])
    except ValueError as e:
        nlog.exception("Invalid node feature in {}:",
                       args.config_file)
        return INVALID_FEATURE

    try:
        slurpy.check_job_properties(job_config['properties'])
    except ValueError as e:
        jlog.exception("Invalid job property in {}:",
                       args.config_file)
        return INVALID_PROPERTY

    try:
        node_writer = df_writer(node_config['out_format'])
    except KeyError as e:
        nlog.exception("Invalid node format in {}:",
                       args.config_file)
        return INVALID_FORMAT

    if args.dry_run:
        nlog.info("--dry-run flag set, no files being saved")
        node_writer = df_writer('none')
    else:
        nlog.info("Saving nodes to directory {}", node_config['out_dir'])

    scheduler = BlockingScheduler(timezone='Australia/Adelaide')

    scheduler.add_job(node_track, 'cron',
                      args=[node_config, node_writer],
                      max_instances=2,
                      **get_cron_freq(node_config))

    scheduler.start()


def node_track(node_config, node_writer):
    nlog = get_slurpyd_logger(NODE_LOG)

    nlog.info("Querying SLURM nodes")
    node_filename = datetime.now().strftime(node_config['out_file'])

    node_time_s = tick()
    rnode_df = slurpy.query_nodes(node_config['features'])
    node_time_s = tick() - node_time_s

    nlog.debug("Querying took {:.3f} ms", node_time_s*S_TO_MS)

    rnodes_path = path_join(node_config['out_dir'], node_filename)

    try:
        node_writer(nlog, rnode_df, rnodes_path)
    except FileNotFoundError:
        nlog.exception("Saving to {} failed:", node_config['out_dir'])


def node_merge(node_config):
    nmlog = get_slurpyd_logger(MRGE_LOG)

    nmlog.info("Merging ")


def setup_loggers(args, log_config):
    # logging settings
    logging._srcfile = None
    logging.logThreads = 0
    logging.logProcesses = 0

    if args.log_file is None:
        log_filename = log_config['output']
    else:
        log_filename = args.log_file

    slurpy_logger = logging.getLogger(MAIN_LOG)
    slurpy_logger.setLevel(logging.DEBUG)

    file_fmtr = logging.Formatter(fmt=log_config['format'], style='{')
    stream_fmtr = logging.Formatter(fmt=STREAM_FMT, style='{')

    rfh = RotatingFileHandler(filename=expanduser(log_filename),
                              mode='w',
                              maxBytes=log_config.getint('max_bytes'),
                              backupCount=log_config.getint('backups'))
    rfh.setLevel(logging.DEBUG)
    rfh.setFormatter(file_fmtr)
    slurpy_logger.addHandler(rfh)

    if args.verbosity > 0:
        sh = logging.StreamHandler()
        sh.setLevel(VERBOSE_LEVEL.get(args.verbosity, logging.DEBUG))
        sh.setFormatter(stream_fmtr)
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

    parser.add_argument('--log-file',
                        type=str,
                        dest='log_file',
                        help='specify custom log file location')

    parser.add_argument('-d', '--dry-run',
                        action='store_true',
                        dest='dry_run',
                        help='run slurpyd without writing files')

    parser.add_argument('-v', '--verbose',
                        action='count',
                        default=0,
                        dest='verbosity',
                        help='specify level of verbosity')

    version_str = '%(prog)s v{}'.format(slurpy.__version__)
    parser.add_argument('--version',
                        action='version',
                        version=version_str)

    return parser


def df_writer(method):
    return {'csv':  _csv,
            'pkl':  _pkl,
            'json': _json,
            'none': _none}.get(method)


def _pkl(log, dataframe, path):
    filepath = '{}.pkl'.format(path)
    _log_save(log, filepath)
    dataframe.to_pickle(filepath)


def _csv(log, dataframe, path):
    filepath = '{}.csv'.format(path)
    _log_save(log, filepath)
    dataframe.to_csv(filepath,
                     index=False)


def _json(log, dataframe, path):
    filepath = '{}.json'.format(path)
    _log_save(log, filepath)
    dataframe.to_json(filepath)


def _none(log, dataframe, path):
    pass


def _log_save(log, path):
    log.info("Saving file to {}", basename(path))


def _keep_first(x):
    return x.iloc[0]


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
