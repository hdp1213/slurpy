from .slurm import query_nodes, query_jobs, NODE_STATES

import numpy as np
import pandas as pd

NODE_FEATURES = ['NodeName',
                 'CPUAlloc',
                 'CPUErr',
                 'CPUTot',
                 'RealMemory',
                 'AllocMem',
                 'FreeMem',
                 'State']

JOB_FEATURES = ['JOBID',
                'PARTITION',
                'ACCOUNT',
                'CPUS',
                'NODES',
                'NODELIST',
                'ST',
                'SUBMIT_TIME']


def main():
    node_df = query_nodes(NODE_FEATURES)

    print(filter_node_states(node_df, ['DOWN'], exclude=True))


def filter_node_states(node_df, states, exclude=False):
    """Filter nodes depending on their current state.

    Returns a new DataFrame with filtered nodes."""
    for state in states:
        if state.upper() not in NODE_STATES:
            raise ValueError('{} not valid state'.format(state.upper()))

    cond_states = '|'.join(['{}'.format(state.upper())
                            for state in states])
    regex = r'.*({}).*'.format(cond_states)

    excl_states = node_df['State'].str.extract(regex, expand=False) \
                                  .isnull()

    if exclude:
        return node_df[excl_states].copy()
    else:
        return node_df[~excl_states].copy()


def get_node_df(node_features=NODE_FEATURES):
    raw_node_df = query_nodes(node_features)
    return _clean_node_df(raw_node_df)


def get_job_df(job_features=JOB_FEATURES):
    raw_job_df = query_jobs(job_features)
    return _clean_job_df(raw_job_df)


def _clean_node_df(node_df):
    node_df['CPUAlloc'] = node_df['CPUAlloc'].astype(int)
    node_df['CPUErr'] = node_df['CPUErr'].astype(int)
    node_df['CPUTot'] = node_df['CPUTot'].astype(int)

    node_df['RealMemory'] = node_df['RealMemory'].astype(int)
    node_df['AllocMem'] = node_df['AllocMem'].astype(int)
    node_df['FreeMem'] = np.where(node_df['FreeMem'] == 'N/A',
                                  0,
                                  node_df['FreeMem']).astype(int)

    return node_df


def _clean_job_df(job_df):
    job_df['CPUS'] = job_df['CPUS'].astype(int)
    job_df['NODES'] = job_df['NODES'].astype(int)
    job_df['SUBMIT_TIME'] = pd.to_datetime(job_df['SUBMIT_TIME'])

    return job_df


if __name__ == '__main__':
    exit(main())
