import slurpy

import logging
from logging.handlers import RotatingFileHandler
from apscheduler.schedulers.blocking import BlockingScheduler

from argparse import ArgumentParser
from configparser import ConfigParser, ExtendedInterpolation

from datetime import datetime, timedelta
from os import remove, walk
from os.path import join as path_join, expanduser, basename, splitext
import signal
from tarfile import open as tar_open
from time import perf_counter as tick

CONFIG_ROOT = '~/.config/slurpyd.ini'

MAIN_LOG = 'slurpyd'
NODE_LOG = 'slurpyd.NodeTrack'
MRGE_LOG = 'slurpyd.MergeNode'
JOBS_LOG = 'slurpyd.JobOrders'

VERBOSE_LEVEL = {1: logging.INFO,
                 2: logging.DEBUG}

BYTE_SUFFIXES = {'G': 30,
                 'M': 20,
                 'K': 10,
                 'B': 0}

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

WRITING_FILES = []
COMPRESSING_FILES = []


def main():
    signal.signal(signal.SIGTERM, graceful_exit)
    signal.signal(signal.SIGINT, graceful_exit)

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
    slog.info("Writing logs to {}",
              args.log_file if args.log_file else log_config['output'])

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
        nlog.info("Will write nodes to directory {} in {} format",
                  node_config['out_dir'],
                  node_config['out_format'])

        mlog.info("Will compress node files to directory {} using "
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


def merge_node(node_config, merge_config, merge_compressor,
               end_time_eval=datetime.now):
    mlog = get_slurpyd_logger(MRGE_LOG)

    end_time = end_time_eval().replace(microsecond=0)
    start_time = end_time - get_timedelta(merge_config)

    mlog.info("Gathering node files written from {:%Y-%m-%d %H:%M:%S} "
              "to {:%Y-%m-%d %H:%M:%S}",
              start_time, end_time)

    gather_time_s = tick()
    tar_files = get_files_between(start_time, end_time, node_config)
    gather_time_s = tick() - gather_time_s

    mlog.debug("Gathering took {:.3f} ms", gather_time_s*S_TO_MS)

    tar_filename = start_time.strftime(merge_config['out_file'])
    tar_path = path_join(expanduser(merge_config['out_dir']),
                         tar_filename)
    tar_compression = merge_config['out_compression']

    merge_compressor(mlog, tar_files, tar_path, tar_compression)


def setup_loggers(args, log_config):
    # logging settings
    logging._srcfile = None
    logging.logThreads = 0
    logging.logProcesses = 0

    log_filename = args.log_file if args.log_file \
        else log_config['output']

    slurpy_logger = logging.getLogger(MAIN_LOG)
    slurpy_logger.setLevel(logging.DEBUG)

    file_fmtr = logging.Formatter(fmt=log_config['format'], style='{')
    stream_fmtr = logging.Formatter(fmt=STREAM_FMT, style='{')

    max_bytes = _to_bytes(log_config['max_size'])

    rfh = RotatingFileHandler(filename=expanduser(log_filename),
                              mode='w',
                              maxBytes=max_bytes,
                              backupCount=log_config.getint('backups'))
    rfh.setLevel(logging.DEBUG)
    rfh.setFormatter(file_fmtr)
    slurpy_logger.addHandler(rfh)

    if args.verbosity > 0:
        sh = logging.StreamHandler()
        sh.setLevel(VERBOSE_LEVEL.get(args.verbosity, logging.DEBUG))
        sh.setFormatter(stream_fmtr)
        slurpy_logger.addHandler(sh)


def get_slurpyd_logger(logger_name):
    return FormatAdapter(logging.getLogger(logger_name))


def get_cron_freq(opt_config):
    base_unit = opt_config['units']
    cron = '*/{}'.format(opt_config['frequency'])

    base_cron = {base_unit: cron}
    lower_cron = {unit: '0'
                  for unit in _get_lower_cron_units(base_unit)}

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


def graceful_exit(signum, frame):
    slog = get_slurpyd_logger(MAIN_LOG)

    for file in WRITING_FILES:
        slog.error("slurpy failed to write {}, deleting...", file)
        remove(file)

    for file in COMPRESSING_FILES:
        slog.error("slurpy failed to compress {}, deleting...", file)
        remove(file)

    slog.critical("slurpyd received signal {}, exiting...", signum)

    exit(signum)


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
            try:
                file_time = datetime.strptime(_strip_ext(file),
                                              opt_config['out_file'])
            except ValueError:
                continue

            if start_time <= file_time < end_time:
                yield path_join(root, file)


def _strip_ext(path):
    return splitext(path)[0]


def _to_bytes(byte_str):
    """Convert a string of the form [0-9]+[G|M|K|B] to bytes"""
    try:
        scale = 2 ** BYTE_SUFFIXES.get(byte_str[-1])
    except KeyError:
        raise ValueError('{!r} is not valid'.format(byte_str[-1]))

    return scale * int(byte_str[:-1])


def decorate_writer(_plain_writer):
    def _decorated_writer(log, dataframe, output_path):
        WRITING_FILES.append(output_path)
        write_time_s = tick()
        _plain_writer(None, dataframe, output_path)
        write_time_s = tick() - write_time_s
        WRITING_FILES.pop()

        log.debug("Writing took {:.3f} ms", write_time_s*S_TO_MS)

    return _decorated_writer


def df_writer(method):
    def write_method(log, dataframe, path):
        file_path = '{}.{}'.format(path, method)
        _log_write(log, file_path)
        _writer = {'csv':  _csv_writer,
                   'pkl':  _pkl_writer,
                   'json': _json_writer,
                   'none': _none_writer}.get(method)

        _writer(log, dataframe, file_path)

    return write_method


@decorate_writer
def _pkl_writer(log, dataframe, output_path):
    dataframe.to_pickle(output_path)


@decorate_writer
def _csv_writer(log, dataframe, output_path):
    dataframe.to_csv(output_path,
                     index=False)


@decorate_writer
def _json_writer(log, dataframe, output_path):
    dataframe.to_json(output_path)


def _none_writer(log, dataframe, output_path):
    pass


def decorate_compressor(_plain_compressor):
    def _decorated_compressor(log, files, output_path, ext, filter):
        COMPRESSING_FILES.append(output_path)
        compress_time_s = tick()
        num_files = _plain_compressor(None, files, output_path, ext,
                                      filter)
        compress_time_s = tick() - compress_time_s
        COMPRESSING_FILES.pop()

        log.info("Compressed {} file(s)", num_files)
        log.debug("Compression took {:.3f} s", compress_time_s)

    return _decorated_compressor


def df_compressor(method):
    def compress_method(log, files, path, compression):
        ext = COMPRESS_EXT.get(compression)
        file_path = '{}.tar.{}'.format(path, ext)
        tar_name = basename(path)

        def _tarball_file(tarinfo):
            tarinfo.name = path_join(tar_name, basename(tarinfo.name))
            return tarinfo

        _log_write(log, file_path)
        _compressor = {'tar':  _tar_compressor,
                       'none': _none_compressor}.get(method)

        _compressor(log, files, file_path, ext, _tarball_file)

    return compress_method


@decorate_compressor
def _tar_compressor(log, files, output_path, ext, filter):
    num_files = 0
    with tar_open(output_path, mode='w:{}'.format(ext)) as tar_file:
        for file in files:
            tar_file.add(file, filter=filter)
            remove(file)
            num_files += 1

    return num_files


def _none_compressor(log, files, output_path, ext, filter):
    return 0


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
