import slurpy

def query_jobs():
    print(repr(slurpy.query_jobs(['JOBID', 'ST', 'SUBMIT_TIME'])))
