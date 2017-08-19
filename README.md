# `slurpy`

A Python 3 API for calling SLURM frontend programs.

## Installation

Install locally using

```bash
$ git clone https://github.com/hdp1213/slurpy.git /path/to/slurpy
$ pip install --prefix ~/.local -e /path/to/slurpy
```

Depending on the state of your `~/.local` directory, you may be required by `pip` to do a bit more legwork to get a working installation. Towards that aim, make sure the directory `~/.local/lib/python3.5/site-packages/` exists before installation.


## Requirements

`slurpy` requires

 - `numpy`
 - `pandas`
 - `pytest` (optional)
