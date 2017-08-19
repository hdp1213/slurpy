from setuptools import setup

setup(name='slurpy',
      version='0.1',
      description='Python API for SLURM frontend commands',
      url='https://github.com/hdp1213/slurpy',
      author='Harry Poulter',
      author_email='hdp1213@hotmail.com',
      license='MIT',
      packages=['slurpy'],
      install_requires=[
          'numpy',
          'pandas',
          'pytest',
      ],
      entry_points={
          'console_scripts': ['query-jobs=slurpy.command_line:query_jobs'],
      },
      zip_safe=False)
