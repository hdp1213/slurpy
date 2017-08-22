from .slurm import query_nodes, query_jobs

from datetime import datetime

import numpy as np
import pandas as pd

ALLOC = 0
IDLE = 1
OTHER = 2
TOTAL = 3


def filter_df(df, column, patterns, exclude=False):
    """Filter DataFrame on a column.

    Returns a new DataFrame with filtered entries."""
    cond_patts = '|'.join(['{}'.format(patt) for patt in patterns])
    regex = r'.*({}).*'.format(cond_patts)

    excl_states = df[column].str.extract(regex, expand=False) \
                                .isnull()

    if exclude:
        return df[excl_states].copy()
    else:
        return df[~excl_states].copy()


def get_node_df(node_features, partition=None):
    raw_node_df = query_nodes(node_features,
                              partition=partition)
    return _clean_node_df(raw_node_df)


def get_job_df(job_features, partition=None, state=None,
               end_time=None, period=None):
    raw_job_df = query_jobs(job_features,
                            partition=partition,
                            state=state,
                            end_time=end_time,
                            period=period)
    return _clean_job_df(raw_job_df)


def _clean_node_df(node_df):
    cpu_aiot = _split_aiot(node_df['CPUsState'])
    del node_df['CPUsState']

    node_df['CPUAlloc'] = cpu_aiot[:, ALLOC]
    node_df['CPUIdle'] = cpu_aiot[:, IDLE]
    node_df['CPUOther'] = cpu_aiot[:, OTHER]
    node_df['CPUTotal'] = cpu_aiot[:, TOTAL]

    node_df['Memory'] = node_df['Memory'].astype(int)
    node_df['AllocMem'] = node_df['AllocMem'].astype(int)
    node_df['FreeMem'] = np.where(node_df['FreeMem'] == 'N/A',
                                  0,
                                  node_df['FreeMem']).astype(int)

    return node_df


def _clean_job_df(job_df):
    job_df['ReqCPUS'] = job_df['ReqCPUS'].astype(int)
    job_df['AllocCPUS'] = job_df['AllocCPUS'].astype(int)
    job_df['AllocNodes'] = job_df['AllocNodes'].astype(int)
    job_df['Submit'] = pd.to_datetime(job_df['Submit'])
    # job_df['Start'] = pd.to_datetime(job_df['Start'])
    job_df['ElapsedRaw'] = job_df['ElapsedRaw'].astype(int)
    job_df['CPUTimeRAW'] = job_df['CPUTimeRAW'].astype(int)

    return job_df


def _split_aiot(aiot):
    return aiot.str.split('/', n=4, expand=True).astype(int).values


if __name__ == '__main__':
    exit(main())
