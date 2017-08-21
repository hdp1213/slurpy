import slurpy


def query_jobs():
    jobs = slurpy.get_job_df()
    jobs['JOBS'] = 1
    print(jobs.groupby('ST').sum())


def query_nodes():
    nodes = slurpy.get_node_df()
    nodes['Nodes'] = 1
    print(nodes.groupby('State').sum())
