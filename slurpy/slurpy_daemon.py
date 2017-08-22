import slurpy

import logging
from logging.handlers import RotatingFileHandler
from apscheduler.schedulers.blocking import BlockingScheduler

from argparse import ArgumentParser
from configparser import ConfigParser, ExtendedInterpolation

from datetime import datetime, timedelta
from os import remove, walk
from os.path import join as path_join, expanduser, basename, splitext
from tarfile import open as tar_open
from time import perf_counter as tick

CONFIG_ROOT = '~/.config/slurpyd.ini'

MAIN_LOG = 'slurpyd'
NODE_LOG = 'slurpyd.NodeTrack'
MRGE_LOG = 'slurpyd.MergeNode'
JOBS_LOG = 'slurpyd.JobOrders'

VERBOSE_LEVEL = {1: logging.INFO,
                 2: logging.DEBUG}

CRON_TO_TD = {'week':   'weeks',
              'day':    'days',
              'hour':   'hours',
              'minute': 'minutes',
              'second': 'seconds'}

CRON_ORDER = ['week', 'day', 'hour', 'minute', 'second']

COMPRESS_EXT = {'gzip':  'gz',
                'bzip2': 'bz2',
                'lzma':  'xz'}

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
    merge_config = config['MergeNode']
    job_config = config['JobOrders']

    setup_loggers(args, log_config)

    slog = get_slurpyd_logger(MAIN_LOG)
    nlog = get_slurpyd_logger(NODE_LOG)
    mlog = get_slurpyd_logger(MRGE_LOG)
    jlog = get_slurpyd_logger(JOBS_LOG)

    slog.info("Starting slurpyd ...")

    # Validate config arguments
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

    # Process writer/compressor information from config file
    try:
        node_writer = df_writer(node_config['out_format'])
    except KeyError as e:
        nlog.exception("Invalid node format in {}:",
                       args.config_file)
        return INVALID_FORMAT

    merge_compressor = df_compressor('tar')

    # Print frequency information for jobs
    nlog.info("Running every {} {}",
              node_config['frequency'], node_config['units'])

    mlog.info("Running every {} {}",
              merge_config['frequency'], merge_config['units'])

    # Change writers to none if a dry run is specified
    if args.dry_run:
        slog.info("--dry-run flag set, no files will be written")

        node_writer = df_writer('none')
        merge_compressor = df_compressor('none')

    else:
        nlog.info("Writing nodes to directory {} in {} format",
                  node_config['out_dir'],
                  node_config['out_format'])

        mlog.info("Compressing node files to directory {} using "
                  "{} compression",
                  merge_config['out_dir'],
                  merge_config['out_compression'])

    # Assign jobs to scheduler
    scheduler = BlockingScheduler(timezone='Australia/Adelaide')

    scheduler.add_job(node_track, 'cron',
                      args=[node_config, node_writer],
                      max_instances=2,
                      **get_cron_freq(node_config))

    scheduler.add_job(merge_node, 'cron',
                      args=[node_config, merge_config,
                            merge_compressor],
                      max_instances=2,
                      **get_cron_freq(merge_config))

    # Start daemon process
    scheduler.start()


def node_track(node_config, node_writer):
    nlog = get_slurpyd_logger(NODE_LOG)

    nlog.info("Querying SLURM nodes")
    node_filename = datetime.now().strftime(node_config['out_file'])

    node_time_s = tick()
    rnode_df = slurpy.query_nodes(node_config['features'])
    node_time_s = tick() - node_time_s

    nlog.debug("Querying took {:.3f} ms", node_time_s*S_TO_MS)

    rnodes_path = path_join(expanduser(node_config['out_dir']),
                            node_filename)

    try:
        node_writer(nlog, rnode_df, rnodes_path)
    except FileNotFoundError:
        nlog.exception("Saving to {} failed:", node_config['out_dir'])


def merge_node(node_config, merge_config, merge_compressor):
    mlog = get_slurpyd_logger(MRGE_LOG)

    end_time = datetime.now()
    start_time = end_time - get_timedelta(merge_config)

    mlog.info("Gathering node files written from {:%Y-%m-%d %H:%M:%S} "
              "to {:%Y-%m-%d %H:%M:%S}",
              start_time, end_time)

    tar_time_s = tick()
    tar_files = get_files_between(start_time, end_time, node_config)
    tar_time_s = tick() - tar_time_s

    mlog.debug("Gathering took {:.3f} ms", tar_time_s*S_TO_MS)

    tar_filename = end_time.strftime(merge_config['out_file'])
    tar_path = path_join(expanduser(merge_config['out_dir']),
                         tar_filename)
    tar_compression = merge_config['out_compression']

    merge_compressor(mlog, tar_path, tar_compression, tar_files)


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
    base_unit = opt_config['units']
    cron = '*/{}'.format(opt_config['frequency'])

    base_cron = {base_unit: cron}
    lower_cron = {}

    for unit in _get_lower_cron_units(base_unit):
        lower_cron[unit] = '0'

    return {**base_cron, **lower_cron}


def _get_lower_cron_units(unit):
    cron_ind = CRON_ORDER.index(unit)
    return CRON_ORDER[cron_ind+1:]


def get_timedelta(opt_config):
    td_units = CRON_TO_TD.get(opt_config['units'])
    value = opt_config.getint('frequency')
    try:
        return timedelta(**{td_units: value})
    except TypeError:
        return timedelta(0)


def make_parser():
    parser = ArgumentParser(description='%(prog)s, a slurpy daemon.')

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


def get_files_between(start_time, end_time, opt_config):
    for root, _, files in walk(expanduser(opt_config['out_dir'])):
        for file in sorted(files):
            file_time = datetime.strptime(_strip_ext(file),
                                          opt_config['out_file'])
            if start_time <= file_time < end_time:
                yield path_join(root, file)


def _strip_ext(path):
    return splitext(path)[0]


def df_writer(method):
    return {'csv':  _csv_write,
            'pkl':  _pkl_write,
            'json': _json_write,
            'none': _none_write}.get(method)


def _pkl_write(log, dataframe, path):
    filepath = '{}.pkl'.format(path)
    _log_write(log, filepath)
    write_time_s = tick()
    dataframe.to_pickle(filepath)
    write_time_s = tick() - write_time_s

    log.debug("Writing took {:.3f} ms", write_time_s*S_TO_MS)


def _csv_write(log, dataframe, path):
    filepath = '{}.csv'.format(path)
    _log_write(log, filepath)
    write_time_s = tick()
    dataframe.to_csv(filepath,
                     index=False)
    write_time_s = tick() - write_time_s

    log.debug("Writing took {:.3f} ms", write_time_s*S_TO_MS)


def _json_write(log, dataframe, path):
    filepath = '{}.json'.format(path)
    _log_write(log, filepath)
    write_time_s = tick()
    dataframe.to_json(filepath)
    write_time_s = tick() - write_time_s

    log.debug("Writing took {:.3f} ms", write_time_s*S_TO_MS)


def _none_write(log, dataframe, path):
    pass


def df_compressor(method):
    return {'tar':  _tar_compress,
            'none': _none_compress}.get(method)


def _tar_compress(log, path, compression, files):
    _log_write(log, path)
    ext = COMPRESS_EXT.get(compression)
    compress_time_s = tick()
    with tar_open('{}.tar.{}'.format(path, ext),
                  mode='w:{}'.format(ext)) as tar_file:
        for file in files:
            tar_file.add(file, filter=_keep_basename)
            remove(file)

    compress_time_s = tick() - compress_time_s

    log.debug("Compression took {:.3f} s", compress_time_s)


def _keep_basename(tarinfo):
    tarinfo.name = basename(tarinfo.name)
    return tarinfo


def _none_compress(log, path, compression, files):
    pass


def _log_write(log, path):
    log.info("Writing file to {}", basename(path))


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
