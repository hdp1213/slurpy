import slurpy


def query_jobs():
    jobs = slurpy.get_job_df(partition='cpu')
    jobs['JOBS'] = 1
    print(jobs.groupby('State').sum())

    return 0


def query_nodes():
    nodes = slurpy.get_node_df(partition='cpu')
    nodes['NODES'] = 1
    del nodes['MEMORY']
    print(nodes.groupby('STATE').sum())

    return 0
