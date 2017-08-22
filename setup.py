from setuptools import setup

setup(name='slurpy',
      version=open("slurpy/_version.py").readlines()[-1]
                                        .split()[-1].strip("\"'"),
      description='Python3 API for SLURM frontend commands',
      url='https://github.com/hdp1213/slurpy',
      author='Harry Poulter',
      author_email='hdp1213@hotmail.com',
      license='MIT',
      packages=['slurpy'],
      install_requires=[
          'numpy',
          'pandas',
          'apscheduler',
          'pytest',
      ],
      entry_points={
          'console_scripts': ['query-jobs=slurpy.cl:query_jobs',
                              'query-nodes=slurpy.cl:query_nodes',
                              'slurpyd=slurpy.slurpy_daemon:main'],
      },
      zip_safe=False)
