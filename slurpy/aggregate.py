import abc

import pandas as pd
import numpy as np

ALLOC = 0
IDLE = 1
OTHER = 2
TOTAL = 3


class GenericAggregator(metaclass=abc.ABCMeta):
    """GenericAggregator object"""
    def __init__(self, *arg, **kwargs):
        super(GenericAggregator, self).__init__()

        self._aggregate = {'Timestamp': []}
        self._name = 'generic_agg'

    @abc.abstractmethod
    def agg(self, timestamp, df_to_aggregate):
        return NotImplemented

    def to_csv(self):
        agg_df = (pd.DataFrame.from_dict(self._aggregate)
                              .set_index('Timestamp')
                              .sort_index())

        min_timestamp = agg_df.index[0]
        filename = '{}-{:%Y%m%d_%H%M%S}.csv'.format(self._name, min_timestamp)

        print('Writing to {}'.format(filename))
        agg_df.to_csv(filename)


class NodeAggregator(GenericAggregator):
    """NodeAggregator object

    NodeHost,StateCompact,CPUsState,Memory,AllocMem,FreeMem"""
    def __init__(self, nodes=None):
        super(NodeAggregator, self).__init__()
        if nodes:
            print("Using pattern {} to filter node hosts".format(nodes))
        else:
            print("Not filtering on node hosts")

        self._aggregate['AllocCPUs'] = []
        self._aggregate['IdleCPUs'] = []
        self._aggregate['OtherCPUs'] = []
        self._aggregate['TotalCPUs'] = []

        self._aggregate['AllocMem'] = []
        self._aggregate['FreeMem'] = []
        self._aggregate['TotalMem'] = []

        self._name = 'node_agg'
        self._node_pattern = '({})'.format(nodes)

    def agg(self, timestamp, df_to_aggregate):
        rnode_df = self.filter_nodes(df_to_aggregate, self._node_pattern)

        cpu_aiot = self._split_aiot(rnode_df['CPUsState'])

        self._aggregate['Timestamp'].append(timestamp)

        self._aggregate['AllocCPUs'].append(cpu_aiot[:, ALLOC].sum())
        self._aggregate['IdleCPUs'].append(cpu_aiot[:, IDLE].sum())
        self._aggregate['OtherCPUs'].append(cpu_aiot[:, OTHER].sum())
        self._aggregate['TotalCPUs'].append(cpu_aiot[:, TOTAL].sum())

        self._aggregate['AllocMem'].append(rnode_df['AllocMem'].sum())
        self._aggregate['FreeMem'].append(rnode_df['FreeMem'].sum())
        self._aggregate['TotalMem'].append(rnode_df['Memory'].sum())

    def filter_nodes(self, df, regex):
        matches_regex = (~df['NodeHost'].str
                                        .extract(regex, expand=False)
                                        .isnull())
        return df[matches_regex].copy()

    def _split_aiot(self, aiot):
        return aiot.str.split('/', n=4, expand=True).astype(int).values
