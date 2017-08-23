import os.path as path
import sys
sys.path.insert(0, path.abspath(path.join(path.dirname(__file__), '..')))

# Modules needed for tests
import slurpy.cl as cl
import slurpy.slurm as slurm
import slurpy.slurpy_daemon as slurpy_daemon
