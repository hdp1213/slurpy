import csv
import io
import re
import subprocess
import datetime

import pandas as pd
import numpy as np

DECODE_FORMAT = 'utf-8'
DELIM = '|'
DATE_FORMAT = '%Y-%m-%dT%H:%M:%S'

JOB_FMT = {'ACCOUNT':          r'%a',
           'GRES':             r'%b',
           'MIN_CPUS':         r'%c',
           'MIN_TMP_DISK':     r'%d',
           'END_TIME':         r'%e',
           'FEATURES':         r'%f',
           'GROUP_NAME':       r'%g',
           'OVER_SUBSCRIBE':   r'%h',
           'JOBID':            r'%i',
           'NAME':             r'%j',
           'COMMENT':          r'%k',
           'TIME_LIMIT':       r'%l',
           'MIN_MEMORY':       r'%m',
           'REQ_NODES':        r'%n',
           'COMMAND':          r'%o',
           'PRIORITY_FLOAT':   r'%p',
           'QOS':              r'%q',
           'REASON':           r'%r',
           'ST':               r'%t',
           'USER':             r'%u',
           'RESERVATION':      r'%v',
           'WCKEY':            r'%w',
           'EXC_NODES':        r'%x',
           'NICE':             r'%y',
           'S:C:T':            r'%z',
           'NUM_TASKS':        r'%A',
           'EXEC_HOST':        r'%B',
           'CPUS':             r'%C',
           'NODES':            r'%D',
           'DEPENDENCY':       r'%E',
           'ARRAY_JOB_ID':     r'%F',
           'GROUP_ID':         r'%G',
           'SOCKETS_PER_NODE': r'%H',
           'CORES_PER_SOCKET': r'%I',
           'THREADS_PER_CORE': r'%J',
           'ARRAY_TASK_ID':    r'%K',
           'TIME_LEFT':        r'%L',
           'TIME':             r'%M',
           'NODELIST':         r'%N',
           'CONTIGUOUS':       r'%O',
           'PARTITION':        r'%P',
           'PRIORITY_INT':     r'%Q',
           'NODELIST(REASON)': r'%R',
           'START_TIME':       r'%S',
           'STATE':            r'%T',
           'USER':             r'%U',
           'SUBMIT_TIME':      r'%V',
           'LICENSES':         r'%W',
           'CORE_SPEC':        r'%X',
           'SCHEDNODES':       r'%Y',
           'WORK_DIR':         r'%Z'}

JOB_STATES = {'PENDING',
              'RUNNING',
              'SUSPENDED',
              'CANCELLED',
              'CONFIGURING',
              'COMPLETING',
              'COMPLETED',
              'FAILED',
              'TIMEOUT',
              'PREEMPTED',
              'NODE_FAIL',
              'REVOKED',
              'SPECIAL_EXIT',
              'BOOT_FAIL',
              'STOPPED'}

JOB_STS = {'PD',
           'R',
           'S',
           'CA',
           'CF',
           'CG',
           'CD',
           'F',
           'TO',
           'PR',
           'NF',
           'RV',
           'SE',
           'BF',
           'ST'}

NODE_FMT = {'AVAIL':             r'%a',
            'ACTIVE_FEATURES':   r'%b',
            'CPUS':              r'%c',
            'TMP_DISK':          r'%d',
            'FREE_MEM':          r'%e',
            'AVAIL_FEATURES':    r'%f',
            'GROUPS':            r'%g',
            'OVERSUBSCRIBE':     r'%h',
            'TIMELIMIT':         r'%l',
            'MEMORY':            r'%m',
            'HOSTNAMES':         r'%n',
            'NODE_ADDR':         r'%o',
            'PRIO_TIER':         r'%p',
            'ROOT':              r'%r',
            'JOB_SIZE':          r'%s',
            'ST':                r'%t',
            'USER':              r'%u',
            'VERSION':           r'%v',
            'WEIGHT':            r'%w',
            'S:C:T':             r'%z',
            'NODES(A/I)':        r'%A',
            'MAX_CPUS_PER_NODE': r'%B',
            'CPUS(A/I/O/T)':     r'%C',
            'NODES':             r'%D',
            'REASON':            r'%E',
            'NODES(A/I/O/T)':    r'%F',
            'GRES':              r'%G',
            'TIMESTAMP':         r'%H',
            'PRIO_JOB_FACTOR':   r'%I',
            'DEFAULTTIME':       r'%L',
            'PREEMPT_MODE':      r'%M',
            'NODELIST':          r'%N',
            'CPU_LOAD':          r'%O',
            'PARTITION*':        r'%P',
            'PARTITION':         r'%R',
            'ALLOCNODES':        r'%S',
            'STATE':             r'%T',
            'USER':              r'%U',
            'SOCKETS':           r'%X',
            'CORES':             r'%Y',
            'THREADS':           r'%Z'}

NODE_STATES = {'UNKNOWN',
               'DOWN',
               'IDLE',
               'ALLOCATED',
               'ERROR',
               'MIXED',
               'FUTURE',
               'DRAIN',
               'DRAINED',
               'DRAINING',
               'NO_RESPOND',
               'RESERVED',
               'PERFCTRS',
               'COMPLETING',
               'POWER_DOWN',
               'POWER_UP',
               'FAIL',
               'MAINT',
               'REBOOT'}


def query_nodes(node_features, **kwargs):
    """Use sinfo to query node features.

    Available node features can be found by reading sinfo man page.

    Returns DataFrame of node features."""
    node_format = ','.join(node_features)
    raw_scontrol = _query_sinfo(node_format, **kwargs)

    return _extract_parsable_data(raw_scontrol,
                                  delimiter=' ',
                                  skipinitialspace=True)


def query_jobs(job_properties, **kwargs):
    """Use sacct to query job properties.

    Available job properties can be found by running `sacct -e`.

    Returns DataFrame of job properties."""
    job_format = ','.join(job_properties)
    raw_sacct = _query_sacct(job_format, **kwargs)

    return _extract_parsable_data(raw_sacct,
                                  delimiter=DELIM)


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


def _extract_parsable_data(raw_data, **csv_kwargs):
    s_data = io.StringIO(raw_data)
    reader = csv.reader(s_data, **csv_kwargs)
    header = next(reader)

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
    sinfo_cmd = ['sinfo', '-O', sinfo_fmt]

    if partition:
        sinfo_cmd += ['-p', partition]

    if node_list:
        sinfo_cmd += ['-n', node_list]

    return subprocess.check_output(sinfo_cmd) \
                     .decode(DECODE_FORMAT)


def _query_sacct(sacct_fmt, partition=None, state=None,
                 start_time=None, end_time=None):
    sacct_cmd = ['sacct', '--units=K', '--delimiter={}'.format(DELIM),
                 '-aPo', sacct_fmt]

    if partition:
        sacct_cmd += ['-r', partition]

    if state:
        sacct_cmd += ['-s', state]

    if start_time:
        sacct_cmd += ['-S', start_time]

    if end_time:
        sacct_cmd += ['-E', end_time]

    return subprocess.check_output(sacct_cmd) \
                     .decode(DECODE_FORMAT)
