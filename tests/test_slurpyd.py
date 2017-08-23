from context import slurpy_daemon
from configparser import ConfigParser, ExtendedInterpolation

import datetime
from glob import glob
import logging
import os
import tarfile

import pytest

CRON_FREQUENCIES = ['1', '10', '-1', '0']

TEST_DIR = '~/slurpy/tests'
NODE_DIR = 'rnodes-20170823_000000'

START_TIME = '2017-08-23 00:00:00'
END_TIME = '2017-08-23 00:01:00'
NODE_FMT = 'rnodes-%Y%m%d_%H%M%S'

COMPRESSION = 'bzip2'
TIMESTAMP = r'%Y-%m-%d %H:%M:%S'


def test_node_track():
    nlog = slurpy_daemon.get_slurpyd_logger(slurpy_daemon.NODE_LOG)

    config = read_config(slurpy_daemon.CONFIG_ROOT)

    node_config = config['NodeTrack']

    node_config['out_dir'] = TEST_DIR
    node_config['out_file'] = 'test_node_track'
    node_config['out_format'] = 'csv'

    nlog.info("Writing nodes to directory {} in {} format",
              node_config['out_dir'],
              node_config['out_format'])

    node_writer = slurpy_daemon.df_writer(node_config['out_format'])

    slurpy_daemon.node_track(node_config, node_writer)

    csv_file = '{out_file}.{out_format}'.format(**node_config)
    csv_path = os.path.join(node_config['out_dir'], csv_file)

    nlog.info("Removing {}".format(csv_path))
    os.remove(os.path.expanduser(csv_path))


def test_merge_node():
    mlog = slurpy_daemon.get_slurpyd_logger(slurpy_daemon.MRGE_LOG)

    config = read_config(slurpy_daemon.CONFIG_ROOT)

    node_config = config['NodeTrack']
    node_config['out_dir'] = os.path.join(TEST_DIR, NODE_DIR)

    merge_config = config['MergeNode']
    merge_config['frequency'] = '1'
    merge_config['units'] = 'minute'
    merge_config['out_dir'] = TEST_DIR
    merge_config['out_compression'] = COMPRESSION

    tar_ext = slurpy_daemon.COMPRESS_EXT.get(COMPRESSION)

    mlog.info("Will compress node files to {} using "
              "{} compression",
              merge_config['out_dir'], merge_config['out_compression'])

    merge_compressor = slurpy_daemon.df_compressor('tar')
    end_time_eval = time_generator(END_TIME)

    slurpy_daemon.merge_node(node_config,
                             merge_config, merge_compressor,
                             end_time_eval)

    tar_path = os.path.join(os.path.expanduser(merge_config['out_dir']),
                            '{}.tar.{}'.format(NODE_DIR, tar_ext))

    mlog.info("Extracting files from {} to {}",
              tar_path, merge_config['out_dir'])
    extract_files(tar_path, tar_ext,
                  os.path.expanduser(merge_config['out_dir']))

    mlog.info("Removing {}".format(tar_path))
    os.remove(os.path.expanduser(tar_path))


@pytest.mark.parametrize('frequency', CRON_FREQUENCIES)
def test_get_cron_freq(frequency):
    config = {}
    config['frequency'] = frequency
    config['units'] = 'week'

    cron_res = slurpy_daemon.get_cron_freq(config)

    assert cron_res == {'week': '*/{}'.format(frequency),
                        'day': '0',
                        'hour': '0',
                        'minute': '0',
                        'second': '0'}


def test_get_files_between():
    config = {}
    config['out_dir'] = os.path.join(TEST_DIR, NODE_DIR)
    config['out_file'] = NODE_FMT

    start_time = time_generator(START_TIME)()
    end_time = time_generator(END_TIME)()

    files = list(slurpy_daemon.get_files_between(start_time, end_time,
                                                 config))

    assert len(files) == 12


def time_generator(time_str):
    def get_time():
        return datetime.datetime.strptime(time_str, TIMESTAMP)
    return get_time


def extract_files(tar_filename, ext, dest_path):
    with tarfile.open(tar_filename, mode='r:{}'.format(ext)) as tar:
        tar.extractall(path=dest_path)


def read_config(path):
    config = ConfigParser(interpolation=ExtendedInterpolation())

    with open(os.path.expanduser(path), mode='r') as conf:
        config.read_file(conf)

    return config


def setup_loggers():
    # logging settings
    logging._srcfile = None
    logging.logThreads = 0
    logging.logProcesses = 0

    slurpy_logger = logging.getLogger(slurpy_daemon.MAIN_LOG)
    slurpy_logger.setLevel(logging.DEBUG)

    file_fmt = '[{asctime}] {name:<18}: {levelname:<8} {message}'
    file_fmtr = logging.Formatter(fmt=file_fmt, style='{')

    sh = logging.StreamHandler()
    sh.setLevel(logging.DEBUG)
    sh.setFormatter(file_fmtr)
    slurpy_logger.addHandler(sh)


setup_loggers()
