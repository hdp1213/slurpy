import slurpy

import argparse
import datetime
import difflib
import os.path
import tarfile

import pandas as pd

CSV_FORMAT = 'rnodes-%Y%m%d_%H%M%S'


def main():
    parser = make_parser()
    args = parser.parse_args()

    path = args.node_path
    aggregator = slurpy.aggregate.NodeAggregator(r'r[1-4]n[0-9]{2}')
    aggregate_csv_archive(path, aggregator)

    return 0


def aggregate_csv_archive(path, aggregator):
    print('Aggregating {}'.format(path))
    try:
        with tarfile.open(path, mode='r:bz2') as tar_file:
            for compressed_file in tar_file:
                file_name = extract_filename(compressed_file.name)
                with tar_file.extractfile(compressed_file) as csv_file:
                    try:
                        node_df = pd.read_csv(csv_file)
                    except pd.io.common.EmptyDataError as e:
                        print('File {}.csv is empty.'.format(file_name),
                              'Skipping...')
                        pass

                    timestamp = datetime.datetime.strptime(file_name,
                                                           CSV_FORMAT)

                    aggregator.agg(timestamp, node_df)

        aggregator.to_csv()
    except FileNotFoundError as e:
        print('File {} not found. Exiting...'.format(path))
        return -1


def extract_filename(file_path):
    return os.path.splitext(os.path.basename(file_path))[0]


def make_parser():
    parser = argparse.ArgumentParser(description='%(prog)s, a slurpy tool.')

    parser.add_argument('node_path',
                        type=str,
                        help='path to file to aggregate')

    version_str = '%(prog)s v{}'.format(slurpy.__version__)
    parser.add_argument('--version',
                        action='version',
                        version=version_str)

    return parser


if __name__ == '__main__':
    exit(main())
