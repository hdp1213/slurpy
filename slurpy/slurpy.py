from .slurm import query_nodes, query_jobs, NODE_STATES

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

    print(filter_states(node_df, ['DOWN'], exclude=True))


def filter_states(node_df, states, exclude=False):
    for state in states:
        if state.upper() not in NODE_STATES:
            raise ValueError('{} not valid state'.format(state.upper()))

    cond_states = '|'.join(['{}'.format(state) for state in states])
    regex = r'.*({}).*'.format(cond_states)

    excl_states = node_df['State'].str.extract(regex, expand=False) \
                                  .isnull()
    if exclude:
        return node_df[excl_states]
    else:
        return node_df[~excl_states]


if __name__ == '__main__':
  exit(main())
