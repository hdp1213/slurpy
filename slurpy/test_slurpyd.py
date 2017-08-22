import slurpy.slurpy_daemon as slurpyd
from slurpy.slurpy_daemon import ConfigParser, ExtendedInterpolation

import logging
import os
from glob import glob

import pytest

CRON_FREQUENCIES = ['1', '10', '-1', '0']


def test_node_track():
    nlog = slurpyd.get_slurpyd_logger(slurpyd.NODE_LOG)

    config = read_config(slurpyd.CONFIG_ROOT)

    node_config = config['NodeTrack']

    node_config['out_dir'] = '~/slurpy'
    node_config['out_file'] = 'test_node_track'
    node_config['out_format'] = 'csv'

    node_writer = slurpyd.df_writer(node_config['out_format'])

    slurpyd.node_track(node_config, node_writer)

    csv_file = '{out_dir}/{out_file}.{out_format}'.format(**node_config)

    nlog.info("Removing {}".format(csv_file))
    os.remove(os.path.expanduser(csv_file))


def test_merge_node():
    mlog = slurpyd.get_slurpyd_logger(slurpyd.MRGE_LOG)

    config = read_config(slurpyd.CONFIG_ROOT)

    node_config = config['NodeTrack']
    merge_config = config['MergeNode']

    merge_config['frequency'] = '20'
    merge_config['units'] = 'second'
    merge_config['out_dir'] = '~/slurpy'
    merge_config['out_file'] = 'test_merge_node'
    merge_config['out_compression'] = 'bzip2'

    mlog.info("Compressing node files to {} using "
              "{} compression",
              merge_config['out_dir'], merge_config['out_compression'])

    merge_compressor = slurpyd.df_compressor('tar')

    slurpyd.merge_node(node_config, merge_config, merge_compressor)

    # TODO: untar the file back to out_dir and then remove the generated one

    # tar_file = '{out_dir}/{out_file}.tar*'.format(**merge_config)

    # mlog.info("Removing {}".format(tar_file))
    # os.remove(glob(os.path.expanduser(tar_file))[0])


@pytest.mark.parametrize('frequency', CRON_FREQUENCIES)
def test_get_cron_freq(frequency):
    config = {}
    config['frequency'] = frequency
    config['units'] = 'week'

    cron_res = slurpyd.get_cron_freq(config)

    assert cron_res == {'week': '*/{}'.format(frequency),
                        'day': '0',
                        'hour': '0',
                        'minute': '0',
                        'second': '0'}


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

    slurpy_logger = logging.getLogger(slurpyd.MAIN_LOG)
    slurpy_logger.setLevel(logging.DEBUG)

    file_fmt = '[{asctime}] {name:<18}: {levelname:<8} {message}'
    file_fmtr = logging.Formatter(fmt=file_fmt, style='{')

    sh = logging.StreamHandler()
    sh.setLevel(logging.DEBUG)
    sh.setFormatter(file_fmtr)
    slurpy_logger.addHandler(sh)


setup_loggers()
