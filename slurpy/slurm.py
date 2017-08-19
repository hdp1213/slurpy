import re
import subprocess

import pandas as pd
import numpy as np

DECODE_FORMAT = 'utf-8'

JOB_FMT = {'ACCOUNT'          : r'%a',
           'GRES'             : r'%b',
           'MIN_CPUS'         : r'%c',
           'MIN_TMP_DISK'     : r'%d',
           'END_TIME'         : r'%e',
           'FEATURES'         : r'%f',
           'GROUP_NAME'       : r'%g',
           'OVER_SUBSCRIBE'   : r'%h',
           'JOBID'            : r'%i',
           'NAME'             : r'%j',
           'COMMENT'          : r'%k',
           'TIME_LIMIT'       : r'%l',
           'MIN_MEMORY'       : r'%m',
           'REQ_NODES'        : r'%n',
           'COMMAND'          : r'%o',
           'PRIORITY_FLOAT'   : r'%p',
           'QOS'              : r'%q',
           'REASON'           : r'%r',
           'ST'               : r'%t',
           'USER'             : r'%u',
           'RESERVATION'      : r'%v',
           'WCKEY'            : r'%w',
           'EXC_NODES'        : r'%x',
           'NICE'             : r'%y',
           'S:C:T'            : r'%z',
           'NUM_TASKS'        : r'%A',
           'EXEC_HOST'        : r'%B',
           'CPUS'             : r'%C',
           'NODES'            : r'%D',
           'DEPENDENCY'       : r'%E',
           'ARRAY_JOB_ID'     : r'%F',
           'GROUP_ID'         : r'%G',
           'SOCKETS_PER_NODE' : r'%H',
           'CORES_PER_SOCKET' : r'%I',
           'THREADS_PER_CORE' : r'%J',
           'ARRAY_TASK_ID'    : r'%K',
           'TIME_LEFT'        : r'%L',
           'TIME'             : r'%M',
           'NODELIST'         : r'%N',
           'CONTIGUOUS'       : r'%O',
           'PARTITION'        : r'%P',
           'PRIORITY_INT'     : r'%Q',
           'NODELIST(REASON)' : r'%R',
           'START_TIME'       : r'%S',
           'STATE'            : r'%T',
           'USER'             : r'%U',
           'SUBMIT_TIME'      : r'%V',
           'LICENSES'         : r'%W',
           'CORE_SPEC'        : r'%X',
           'SCHEDNODES'       : r'%Y',
           'WORK_DIR'         : r'%Z'
          }

JOB_STATES = {'PENDING', 'RUNNING', 'SUSPENDED',  'CANCELLED',  'COMPLETING',  'COMPLETED', 'CONFIGURING', 'FAILED', 'TIMEOUT', 'PREEMPTED', 'NODE_FAIL', 'REVOKED', 'SPECIAL_EXIT', 'BOOT_FAIL', 'STOPPED'}

JOB_STS = {'PD', 'R', 'S', 'CA', 'CF', 'CG', 'CD', 'F', 'TO', 'PR', 'NF', 'RV', 'SE', 'BF', 'ST'} # no COMPLETING?

NODE_STATES = {'UNKNOWN', 'DOWN', 'IDLE', 'ALLOCATED', 'ERROR', 'MIXED', 'FUTURE', 'DRAIN', 'DRAINED', 'DRAINING', 'NO_RESPOND', 'RESERVED', 'PERFCTRS', 'COMPLETING', 'POWER_DOWN', 'POWER_UP', 'FAIL', 'MAINT', 'REBOOT'}


# 211ms ± 13.3ms per loop
def query_nodes(node_features, partition=None):
    raw_scontrol = _query_scontrol()
    return _extract_node_features(raw_scontrol, node_features)


def query_jobs(job_features, partition=None):
    job_format = ','.join([JOB_FMT[feat.upper()]
                           for feat in job_features])
    raw_squeue = _query_squeue(job_format, partition)

    return raw_squeue


# Really only here to keep _extract_node_features() in check
def _extract_node_feature(raw_scontrol, feature):
    matches = re.findall(r'.*{}=([^\s]+).*'.format(feature),
                         raw_scontrol)

    if not matches:
        raise ValueError('{} not found'.format(feature))

    return np.array(matches)


# Properties must be given in the order they appear in scontrol
# 136ms ± 2.47ms per loop
def _extract_node_features(raw_scontrol, features):
    regex = ''.join([r'.*?{}=([^\s]+)'.format(feature)
                     for feature in features]
                    + ['.*?'])

    matches = re.findall(regex, raw_scontrol)
    
    return pd.DataFrame(data=np.array(matches),
                        columns=features)


def _extract_job_data(raw_squeue):
    raw_squeue.splitlines()


def _query_squeue(form, partition=None):
    if partition:
        squeue_cmd = ['squeue', '-p', partition, '-o', form]
    else:
        squeue_cmd = ['squeue', '-o', form]

    raw_squeue = subprocess.check_output(squeue_cmd) \
                           .decode(DECODE_FORMAT)

    return raw_squeue


def _query_scontrol():
    scontrol_cmd = ['scontrol', '-o', 'show', 'node']
    raw_scontrol = subprocess.check_output(scontrol_cmd) \
                             .decode(DECODE_FORMAT)
    return raw_scontrol
