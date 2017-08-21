from .slurm import _extract_scontrol_feature, _extract_scontrol_features
from .cl import *

from numpy import all as np_all

import pytest

NODE_FEATURES = ['NodeName',
                 'CPUAlloc',
                 'CPUTot',
                 'FreeMem',
                 'Sockets',
                 'Boards',
                 'State']

RAW_SCONTROL = 'NodeName=datamover1 Arch=x86_64 CoresPerSocket=1 \
CPUAlloc=23 CPUErr=0 CPUTot=24 CPULoad=0.01 AvailableFeatures=(null) \
ActiveFeatures=(null) Gres=(null) NodeAddr=datamover1 \
NodeHostName=datamover1 Version=16.05 OS=Linux RealMemory=96672 \
AllocMem=0 FreeMem=6374 Sockets=24 Boards=1 State=IDLE ThreadsPerCore=1 \
TmpDisk=0 Weight=100 Owner=N/A MCS_label=N/A Partitions=copy  \
BootTime=2017-05-08T11:58:28 SlurmdStartTime=2017-05-31T09:55:13 \
CfgTRES=cpu=24,mem=96672M AllocTRES= CapWatts=n/a CurrentWatts=0 \
LowestJoules=0 ConsumedJoules=0 ExtSensorsJoules=n/s ExtSensorsWatts=0 \
ExtSensorsTemp=n/s \nNodeName=datamover2 CoresPerSocket=1 CPUAlloc=17 \
CPUErr=0 CPUTot=24 CPULoad=N/A AvailableFeatures=(null) \
ActiveFeatures=(null) Gres=(null) NodeAddr=datamover2 \
NodeHostName=datamover2  RealMemory=1 AllocMem=0 FreeMem=N/A Sockets=24 \
Boards=1 State=DOWN*+DRAIN ThreadsPerCore=1 TmpDisk=0 Weight=100 \
Owner=N/A MCS_label=N/A Partitions=copy  BootTime=None \
SlurmdStartTime=None CfgTRES=cpu=24,mem=1M AllocTRES= CapWatts=n/a \
CurrentWatts=0 LowestJoules=0 ConsumedJoules=0 ExtSensorsJoules=n/s \
ExtSensorsWatts=0 ExtSensorsTemp=n/s \
Reason=Not responding [root@2017-06-29T08:00:07]\nNodeName=highmem1 \
Arch=x86_64 CoresPerSocket=1 CPUAlloc=4 CPUErr=0 CPUTot=32 \
CPULoad=26.73 AvailableFeatures=(null) ActiveFeatures=(null) \
Gres=(null) NodeAddr=highmem1 NodeHostName=highmem1 Version=16.05 \
OS=Linux RealMemory=515747 AllocMem=0 FreeMem=1102 Sockets=32 Boards=1 \
State=DOWN* ThreadsPerCore=1 TmpDisk=0 Weight=100 Owner=N/A \
MCS_label=N/A Partitions=highmem  BootTime=2017-07-13T09:10:39 \
SlurmdStartTime=2017-07-27T10:41:53 CfgTRES=cpu=32,mem=515747M \
AllocTRES= CapWatts=n/a CurrentWatts=0 LowestJoules=0 ConsumedJoules=0 \
ExtSensorsJoules=n/s ExtSensorsWatts=0 ExtSensorsTemp=n/s \
Reason=Not responding [root@2017-08-18T11:14:39]\nNodeName=highmem2 \
Arch=x86_64 CoresPerSocket=1 CPUAlloc=19 CPUErr=0 CPUTot=32 \
CPULoad=16.34 AvailableFeatures=(null) ActiveFeatures=(null) \
Gres=(null) NodeAddr=highmem2 NodeHostName=highmem2 Version=16.05 \
OS=Linux RealMemory=515882 AllocMem=0 FreeMem=1103 Sockets=32 Boards=1 \
State=DOWN* ThreadsPerCore=1 TmpDisk=0 Weight=100 Owner=N/A \
MCS_label=N/A Partitions=highmem  BootTime=2017-07-13T01:01:19 \
SlurmdStartTime=2017-07-13T01:02:55 CfgTRES=cpu=32,mem=515882M \
AllocTRES= CapWatts=n/a CurrentWatts=0 LowestJoules=0 ConsumedJoules=0 \
ExtSensorsJoules=n/s ExtSensorsWatts=0 ExtSensorsTemp=n/s \
Reason=Not responding [root@2017-08-18T11:14:39]\nNodeName=highmem4 \
CoresPerSocket=1 CPUAlloc=30 CPUErr=0 CPUTot=32 CPULoad=N/A \
AvailableFeatures=(null) ActiveFeatures=(null) Gres=(null) \
NodeAddr=highmem4 NodeHostName=highmem4  RealMemory=515754 AllocMem=0 \
FreeMem=N/A Sockets=32 Boards=1 State=DOWN* ThreadsPerCore=1 TmpDisk=0 \
Weight=100 Owner=N/A MCS_label=N/A Partitions=highmem  BootTime=None \
SlurmdStartTime=None CfgTRES=cpu=32,mem=515754M AllocTRES= CapWatts=n/a \
CurrentWatts=0 LowestJoules=0 ConsumedJoules=0 ExtSensorsJoules=n/s \
ExtSensorsWatts=0 ExtSensorsTemp=n/s \
Reason=Not responding [root@2017-08-18T11:14:39]\nNodeName=lm1 \
Arch=x86_64 CoresPerSocket=1 CPUAlloc=29 CPUErr=0 CPUTot=32 \
CPULoad=15.13 AvailableFeatures=(null) ActiveFeatures=(null) \
Gres=(null) NodeAddr=lm1 NodeHostName=lm1  OS=Linux RealMemory=1547638 \
AllocMem=0 FreeMem=5236 Sockets=32 Boards=1 State=DOWN* \
ThreadsPerCore=1 TmpDisk=0 Weight=1000 Owner=N/A MCS_label=N/A \
Partitions=highmem  BootTime=2017-07-27T10:57:23 \
SlurmdStartTime=2017-07-27T11:25:03 CfgTRES=cpu=32,mem=1547638M \
AllocTRES= CapWatts=n/a CurrentWatts=0 LowestJoules=0 ConsumedJoules=0 \
ExtSensorsJoules=n/s ExtSensorsWatts=0 ExtSensorsTemp=n/s \
Reason=Not responding [root@2017-08-18T13:57:33]\nNodeName=lm2 \
CoresPerSocket=1 CPUAlloc=20 CPUErr=0 CPUTot=32 CPULoad=N/A \
AvailableFeatures=(null) ActiveFeatures=(null) Gres=(null) NodeAddr=lm2 \
NodeHostName=lm2  RealMemory=1547638 AllocMem=0 FreeMem=N/A Sockets=32 \
Boards=1 State=DOWN* ThreadsPerCore=1 TmpDisk=0 Weight=1000 Owner=N/A \
MCS_label=N/A Partitions=highmem  BootTime=None SlurmdStartTime=None \
CfgTRES=cpu=32,mem=1547638M AllocTRES= CapWatts=n/a CurrentWatts=0 \
LowestJoules=0 ConsumedJoules=0 ExtSensorsJoules=n/s ExtSensorsWatts=0 \
ExtSensorsTemp=n/s Reason=NHC:  [root@2017-08-07T11:01:52]\n\
NodeName=lm3 Arch=x86_64 CoresPerSocket=1 CPUAlloc=14 CPUErr=0 \
CPUTot=32 CPULoad=1.01 AvailableFeatures=(null) ActiveFeatures=(null) \
Gres=(null) NodeAddr=lm3 NodeHostName=lm3  OS=Linux RealMemory=1547638 \
AllocMem=0 FreeMem=N/A Sockets=32 Boards=1 State=DOWN* ThreadsPerCore=1 \
TmpDisk=0 Weight=1000 Owner=N/A MCS_label=N/A Partitions=highmem  \
BootTime=2017-07-27T10:57:11 SlurmdStartTime=2017-07-28T14:21:58 \
CfgTRES=cpu=32,mem=1547638M AllocTRES= CapWatts=n/a CurrentWatts=0 \
LowestJoules=0 ConsumedJoules=0 ExtSensorsJoules=n/s ExtSensorsWatts=0 \
ExtSensorsTemp=n/s Reason=NHC:  [root@2017-08-06T22:59:45]\n\
NodeName=r1n03 Arch=x86_64 CoresPerSocket=1 CPUAlloc=32 CPUErr=0 \
CPUTot=32 CPULoad=32.01 AvailableFeatures=(null) ActiveFeatures=(null) \
Gres=(null) NodeAddr=r1n03 NodeHostName=r1n03 Version=16.05 OS=Linux \
RealMemory=128246 AllocMem=124928 FreeMem=107532 Sockets=32 Boards=1 \
State=ALLOCATED ThreadsPerCore=1 TmpDisk=238352 Weight=1 Owner=N/A \
MCS_label=N/A Partitions=batch,cpu,test  BootTime=2017-06-30T15:20:09 \
SlurmdStartTime=2017-06-30T15:23:25 CfgTRES=cpu=32,mem=128246M \
AllocTRES=cpu=32,mem=122G CapWatts=n/a CurrentWatts=0 LowestJoules=0 \
ConsumedJoules=0 ExtSensorsJoules=n/s ExtSensorsWatts=0 \
ExtSensorsTemp=n/s \nNodeName=r1n04 Arch=x86_64 CoresPerSocket=1 \
CPUAlloc=32 CPUErr=0 CPUTot=32 CPULoad=8.00 AvailableFeatures=(null) \
ActiveFeatures=(null) Gres=(null) NodeAddr=r1n04 NodeHostName=r1n04 \
Version=16.05 OS=Linux RealMemory=128246 AllocMem=128000 FreeMem=98476 \
Sockets=32 Boards=1 State=ALLOCATED ThreadsPerCore=1 TmpDisk=238352 \
Weight=1 Owner=N/A MCS_label=N/A Partitions=batch,cpu,test  \
BootTime=2017-06-30T15:20:06 SlurmdStartTime=2017-06-30T15:23:21 \
CfgTRES=cpu=32,mem=128246M AllocTRES=cpu=32,mem=125G CapWatts=n/a \
CurrentWatts=0 LowestJoules=0 ConsumedJoules=0 ExtSensorsJoules=n/s \
ExtSensorsWatts=0 ExtSensorsTemp=n/s '

RAW_SCONTROL_LEN = 10


@pytest.mark.parametrize('node_feat', NODE_FEATURES)
def test_compare_extraction(node_feat):
    cpu_alloc = _extract_scontrol_feature(RAW_SCONTROL, node_feat)
    node_info = _extract_scontrol_features(RAW_SCONTROL, [node_feat])

    assert len(cpu_alloc) == RAW_SCONTROL_LEN
    assert len(cpu_alloc) == len(node_info)

    assert np_all(cpu_alloc == node_info[node_feat].values)


def test_extract_scontrol_features():
    node_info = _extract_scontrol_features(RAW_SCONTROL, NODE_FEATURES)

    assert len(node_info) == RAW_SCONTROL_LEN


def test_query_jobs():
    assert query_jobs() == 0


def test_query_nodes():
    assert query_nodes() == 0
