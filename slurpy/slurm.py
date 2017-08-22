import csv
import io
import re
import subprocess
import datetime

import pandas as pd
import numpy as np

DECODE_FORMAT = 'utf-8'
DELIM = '|'
COMMA = ','
DATE_FORMAT = '%Y-%m-%dT%H:%M:%S'

VALID_NODE_FEATURES = {'all',
                       'allocmem',
                       'allocnodes',
                       'available',
                       'cpus',
                       'cpusload',
                       'freemem',
                       'cpusstate',
                       'cores',
                       'defaulttime',
                       'disk',
                       'features',
                       'features_act',
                       'groups',
                       'gres',
                       'maxcpuspernode',
                       'memory',  # mb
                       'nodes',
                       'nodeaddr',
                       'nodeai',
                       'nodeaiot',
                       'nodehost',
                       'nodelist',
                       'oversubscribe',
                       'partition',
                       'partitionname',
                       'port',
                       'preemptmode',
                       'priorityjobfactor',
                       'prioritytier',
                       'priority',
                       'reason',
                       'root',
                       'size',
                       'statecompact',
                       'statelong',
                       'sockets',
                       'socketcorethread',
                       'time',
                       'timestamp',
                       'threads',
                       'user',
                       'userlong',
                       'version',
                       'weight'}

VALID_JOB_PROPERTIES = {'all',
                        'account',
                        'admincomment',
                        'alloccpus',
                        'allocgres',
                        'allocnodes',
                        'alloctres',
                        'associd',
                        'avecpu',
                        'avecpufreq',
                        'avediskread',
                        'avediskwrite',
                        'avepages',
                        'averss',
                        'avevmsize',
                        'blockid',
                        'cluster',
                        'comment',
                        'consumedenergy',
                        'consumedenergyraw',
                        'cputime',
                        'cputimeraw',
                        'derivedexitcode',
                        'elapsed',
                        'elapsedraw',
                        'eligible',
                        'end',
                        'exitcode',
                        'gid',
                        'group',
                        'jobid',
                        'jobidraw',
                        'jobname',
                        'layout',
                        'maxdiskread',
                        'maxdiskreadnode',
                        'maxdiskreadtask',
                        'maxdiskwrite',
                        'maxdiskwritenode',
                        'maxdiskwritetask',
                        'maxpages',
                        'maxpagesnode',
                        'maxpagestask',
                        'maxrss',
                        'maxrssnode',
                        'maxrsstask',
                        'maxvmsize',
                        'maxvmsizenode',
                        'maxvmsizetask',
                        'mincpu',
                        'mincpunode',
                        'mincputask',
                        'ncpus',
                        'nnodes',
                        'nodelist',
                        'ntasks',
                        'priority',
                        'partition',
                        'qos',
                        'qosraw',
                        'reqcpufreq',
                        'reqcpufreqmin',
                        'reqcpufreqmax',
                        'reqcpufreqgov',
                        'reqcpus',
                        'reqgres',
                        'reqmem',
                        'reqnodes',
                        'reqtres',
                        'reservation',
                        'reservationid',
                        'reserved',
                        'resvcpu',
                        'resvcpuraw',
                        'start',
                        'state',
                        'submit',
                        'suspended',
                        'systemcpu',
                        'timelimit',
                        'totalcpu',
                        'uid',
                        'user',
                        'usercpu',
                        'wckey',
                        'wckeyid'}


def query_nodes(node_features, **kwargs):
    """Use sinfo to query node features.

    Available node features can be found by reading sinfo man page.

    Returns DataFrame of node features."""
    node_format, node_header = _listify(node_features)

    raw_sinfo = _query_sinfo(node_format, **kwargs)

    # Need to append empty string to node_header due to csv reading
    # It is removed in the DataFrame construction
    node_header.append('')

    return _extract_parsable_data(raw_sinfo, node_header,
                                  delimiter=' ',
                                  skipinitialspace=True,
                                  strict=True)


def query_jobs(job_properties, **kwargs):
    """Use sacct to query job properties.

    Available job properties can be found by running `sacct -e`.

    Returns DataFrame of job properties."""
    job_format, job_header = _listify(job_properties)

    raw_sacct = _query_sacct(job_format, **kwargs)

    return _extract_parsable_data(raw_sacct, job_header,
                                  delimiter=DELIM,
                                  strict=True)


def check_job_properties(properties):
    if isinstance(properties, str):
        properties = properties.split(COMMA)

    for prop in properties:
        if prop.lower() not in VALID_JOB_PROPERTIES:
            err_str = '{!r} is not a valid job property'.format(prop)
            raise ValueError(err_str)


def check_node_features(features):
    if isinstance(features, str):
        features = features.split(COMMA)

    for feat in features:
        if feat.lower() not in VALID_NODE_FEATURES:
            err_str = '{!r} is not a valid node feature'.format(feat)
            raise ValueError(err_str)


# Really only here to keep _extract_scontrol_features() in check
def _extract_scontrol_feature(raw_scontrol, feature):
    matches = re.findall(r'.*{}=([^\s]+).*'.format(feature),
                         raw_scontrol)

    if not matches:
        raise ValueError('{} not found'.format(feature))

    return np.array(matches)


# Properties must be given in the order they appear in scontrol
def _extract_scontrol_features(raw_scontrol, features):
    regex = ''.join([r'.*?{}=([^\s]+)'.format(feature)
                     for feature in features] + ['.*?'])

    matches = re.findall(regex, raw_scontrol)

    return pd.DataFrame(data=np.array(matches),
                        columns=features)


def _extract_parsable_data(raw_data, header, **csv_kwargs):
    s_data = io.StringIO(raw_data)
    reader = csv.reader(s_data, **csv_kwargs)

    return pd.DataFrame(data=np.array([row for row in reader]),
                        columns=header)


def _query_squeue(squeue_fmt, partition=None):
    if partition:
        squeue_cmd = ['squeue', '-p', partition, '-o', squeue_fmt]
    else:
        squeue_cmd = ['squeue', '-o', squeue_fmt]

    return subprocess.check_output(squeue_cmd) \
                     .decode(DECODE_FORMAT)


def _query_scontrol(entity, ID=''):
    scontrol_cmd = ['scontrol', '-o', 'show', entity, ID]

    return subprocess.check_output(scontrol_cmd) \
                     .decode(DECODE_FORMAT)


def _query_sinfo(sinfo_fmt, partition=None, node_list=None):
    sinfo_cmd = ['sinfo', '--noconvert', '--noheader',
                 '-O', sinfo_fmt]

    if partition:
        sinfo_cmd += ['-p', partition]

    if node_list:
        sinfo_cmd += ['-n', node_list]

    return subprocess.check_output(sinfo_cmd) \
                     .decode(DECODE_FORMAT)


def _query_sacct(sacct_fmt, partition=None, state=None,
                 end_time=None, period=None):
    sacct_cmd = ['sacct', '--units=K', '--delimiter={}'.format(DELIM),
                 '--noheader', '-aPo', sacct_fmt]

    if partition:
        sacct_cmd += ['-r', partition]

    if state:
        sacct_cmd += ['-s', state]

    if end_time:
        sacct_cmd += ['-E', end_time.strftime(DATE_FORMAT)]

        if period:
            start_time = end_time - period
            sacct_cmd += ['-S', start_time.strftime(DATE_FORMAT)]

    elif period:
        raise ValueError('Cannot specify period without an end time!')

    return subprocess.check_output(sacct_cmd) \
                     .decode(DECODE_FORMAT)


def _listify(x):
    """Return a (string, tuple) representation of x"""
    try:
        return x, x.split(COMMA)
    except AttributeError:
        return COMMA.join(x), x
