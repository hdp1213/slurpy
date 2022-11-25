from context import slurpy_daemon
from configparser import ConfigParser, ExtendedInterpolation

import datetime
from glob import glob
import logging
import os
import tarfile

import pytest

CRON_FREQUENCIES = ['1', '10', '-1', '0']

BYTE_PAIRS = [('1B', 1),
              ('3K', 3072),
              ('40M', 41943040),
              ('7G', 7516192768)]

TEST_NODE_CONFIG = '~/slurpy/tests/test_node_track.ini'
TEST_MERGE_CONFIG = '~/slurpy/tests/test_merge_node.ini'
TEST_DIR = '~/slurpy/tests'
NODE_DIR = 'rnodes-20170823_000000'

START_TIME = '2017-08-23 00:00:00'
END_TIME = '2017-08-23 00:01:00'
NODE_FMT = 'rnodes-%Y%m%d_%H%M%S'

TIMESTAMP = r'%Y-%m-%d %H:%M:%S'


def test_node_track():
    nlog = slurpy_daemon.get_slurpyd_logger(slurpy_daemon.NODE_LOG)

    config = slurpy_daemon.read_config(TEST_NODE_CONFIG)

    node_config = config['NodeTrack']

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

    config = slurpy_daemon.read_config(TEST_MERGE_CONFIG)

    node_config = config['NodeTrack']
    merge_config = config['MergeNode']

    tar_ext = slurpy_daemon.COMPRESS_EXT.get(merge_config['out_compression'])

    mlog.info("Will compress node files to {} using "
              "{} compression",
              merge_config['out_dir'], merge_config['out_compression'])

    merge_compressor = slurpy_daemon.df_compressor('tar')
    end_time_eval = time_generator(END_TIME)

    slurpy_daemon.merge_node(node_config,
                             merge_config, merge_compressor,
                             end_time_eval)

    tar_path = os.path.join(merge_config['out_dir_sh'],
                            '{}.tar.{}'.format(NODE_DIR, tar_ext))

    mlog.info("Extracting files from {} to {}",
              tar_path, merge_config['out_dir'])
    extract_files(tar_path, tar_ext, merge_config['out_dir_sh'])

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
    config['out_dir_sh'] = slurpy_daemon._expand_path(config['out_dir'])
    config['out_file'] = NODE_FMT

    start_time = time_generator(START_TIME)()
    end_time = time_generator(END_TIME)()

    files = list(slurpy_daemon.get_files_between(start_time, end_time,
                                                 config))

    assert len(files) == 12


@pytest.mark.parametrize('test_pair', BYTE_PAIRS)
def test_to_bytes(test_pair):
    assert slurpy_daemon._to_bytes(test_pair[0]) == test_pair[1]


def time_generator(time_str):
    def get_time():
        return datetime.datetime.strptime(time_str, TIMESTAMP)
    return get_time


def extract_files(tar_filename, ext, dest_path):
    with tarfile.open(tar_filename, mode='r:{}'.format(ext)) as tar:
        def is_within_directory(directory, target):
            
            abs_directory = os.path.abspath(directory)
            abs_target = os.path.abspath(target)
        
            prefix = os.path.commonprefix([abs_directory, abs_target])
            
            return prefix == abs_directory
        
        def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
        
            for member in tar.getmembers():
                member_path = os.path.join(path, member.name)
                if not is_within_directory(path, member_path):
                    raise Exception("Attempted Path Traversal in Tar File")
        
            tar.extractall(path, members, numeric_owner=numeric_owner) 
            
        
        safe_extract(tar, path=dest_path)


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
