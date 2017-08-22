import slurpy

import datetime
import sys

NOW = datetime.datetime.now()


def query_jobs(**td_kwargs):
    properties = ['JobID',
                  'State',
                  'ReqCPUS',
                  'AllocCPUS',
                  'AllocNodes',
                  'NodeList',
                  'NTasks',
                  'Submit',
                  'Start',
                  'ElapsedRaw',
                  'CPUTimeRAW',
                  'MaxRSS',
                  'ReqMem']

    # TODO: fix this horrible mess
    if not td_kwargs:
        if len(sys.argv) > 1:
            amount = int(sys.argv[1])
        else:
            amount = 1

        if len(sys.argv) > 2:
            unit = str(sys.argv[2])
        else:
            unit = 'hours'

        td_kwargs = {unit: amount}
    else:
        unit, amount = list(td_kwargs.items())[0]

    jobs = slurpy.get_job_df(properties,
                             partition='cpu',
                             end_time=NOW,
                             period=datetime.timedelta(**td_kwargs))
    jobs['Jobs'] = 1

    print('In last {} {}:'.format(amount, unit))
    print('Most recent start time: {}'.format(jobs['Start'].max()))
    print('Most recent submit time: {}'.format(jobs['Submit'].max()))
    print(jobs.groupby('State').sum())

    return 0


def query_nodes():
    features = ['NodeHost',
                'StateCompact',
                'CPUsState',
                'Memory',
                'AllocMem',
                'FreeMem']

    nodes = slurpy.get_node_df(features, partition='cpu')
    nodes['Nodes'] = 1
    del nodes['Memory']
    print(nodes.groupby('StateCompact').sum())

    return 0
