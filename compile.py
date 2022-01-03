#!/usr/bin/python3

import argparse
import json
import os
import re

NoValue = '<no-value>'

GistStatFields = [
    "Number of levels",
    "Number of pages",
    "Number of leaf pages",
    "Number of tuples",
    "Number of invalid tuples",
    "Number of leaf tuples",
    "Total size of tuples",
    "Total size of leaf tuples",
    "Total size of index"
]

Tests = [
    {'name': 'Create index', 'key': 'creat_index_ms'},
    {'name': 'Self-join', 'key': 'self_join'},
    {'name': 'Tiling', 'key': 'tiling_ms'},
    {'name': 'kNN k=1', 'key': 'knn_1_ms'},
    {'name': 'kNN k=100', 'key': 'knn_100_ms'},
]

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('paths', nargs='+')
    parser.add_argument('--labels', nargs='*')
    return parser.parse_args()

def load_data(path):
    with open(path, 'r') as fd:
        return json.load(fd)

def get_value(data, field, default=None):
    keys = field.split('/')
    value = data
    for key in keys:
        if not isinstance(value, dict) or key not in value:
            return default
        value = value[key]
    return value

def out(text):
    print(text, end='')

def nl():
    print()

def print_header(labels):
    out('| |')
    for label in labels:
        out(' {} |'.format(label))
    nl()
    out('|-|' + '-|'*len(labels))
    nl()

def print_gist_stats(data, labels):
    # Header
    print_header(labels)
    # Data
    for field in GistStatFields:
        # Label
        out('| {} |'.format(field))
        # Values
        for fdata in data:
            out(' {} |'.format(get_value(fdata, 'gist_stats/'+field, NoValue)))
        nl()
    # Effective fill
    out('| Effective fill |')
    for fdata in data:
        tuple_size = get_value(fdata, 'gist_stats/Total size of tuples')
        index_size = get_value(fdata, 'gist_stats/Total size of index')
        if (tuple_size is not None) and index_size:
            out(' {} |'.format(round(size_to_int(tuple_size) / size_to_int(index_size), 4)))
        else:
            out(' N/A |')
    nl()

def size_to_int(value):
    match = re.match('(\d+)\s+bytes', value)
    size = match[1] if (match) else value;
    return int(size)

def print_tests(data, labels):
    # Header
    print_header(labels)
    # Data
    for test in Tests:
        # Get values
        values = [get_value(fdata, test['key']) for fdata in data]
        # Check if test was omitted
        if all(value is None for value in values):
            continue

        # Label
        out('| {} |'.format(test['name']))
        # Data
        for value in values:
            if value is not None:
                out(' {} / {} |'.format(value['mean'], value['median']))
            else:
                out(' {} |'.format(NoValue))
        nl()

def filename_no_ext(path):
    basename = os.path.basename(path)
    return os.path.splitext(basename)[0]

def main():
    # Parse args
    args = parse_args()

    # Load data
    data = [load_data(path) for path in args.paths]

    # Get labels for all data files, filename with no ext by default
    labels = []
    for i in range(len(args.paths)):
        labels.append(args.labels[i] if args.labels and len(args.labels) > i else filename_no_ext(args.paths[i]))

    # Print tables
    print_gist_stats(data, labels)
    nl()
    print_tests(data, labels)

if __name__ == '__main__':
    main()
