"""A speedy Python 3 API for SLURM frontend commands

slurpy provides a fast way to query job and node status through SLURM"""

from slurpy.slurpy import filter_df, get_node_df, get_job_df
from slurpy.slurm import query_nodes, query_jobs, \
                         check_node_features, check_job_properties
import slurpy.aggregate as aggregate
from slurpy._version import __version__

__all__ = (
    filter_df,
    get_node_df,
    get_job_df,
    query_nodes,
    query_jobs,
    check_node_features,
    check_job_properties
)
