#!/usr/bin/env python3

import json
import sys
import argparse
import re
from argparse import RawTextHelpFormatter
from jsonfilter.match import *

enabled_colors = False
try:
    from pygments import highlight
    from pygments.lexers import JsonLexer
    from pygments.formatters import TerminalFormatter
    enabled_colors = True
except ImportError:
    pass


def pretty_json(data):
    """
    Returns JSON-dumped dict
    :param data:
    :return:
    """
    return json.dumps(
        data,
        ensure_ascii=False, indent=4, separators=(',', ': '), sort_keys=True
    )


def error_json(message):
    """
    Returns pretty JSON-form error message
    :param message: str
    :return: str
    """
    return pretty_json({'error': message})


def pretty_printable(iterable_data, colorize=False):
    """
    Pretty printing JSON document
    :param iterable_data: filter or list
    :param colorize: bool - colorize output if possible
    """
    result = pretty_json(list(iterable_data))
    if colorize and not enabled_colors:
        raise JFError("Can't import pygments module")

    if colorize:
        return highlight(result, JsonLexer(), TerminalFormatter())

    return result


def read_data_from_stdin():
    try:
        return json.loads(sys.stdin.read())
    except Exception:
        raise JFError("Can't get correct JSON from stdin")


def load_data_from_file(path):
    try:
        with open(path) as f:
            return json.load(f)
    except IOError:
        raise JFError("Can't load file {}".format(
            args.filter_file
        ))
    except Exception:
        raise JFError("Can't read filters from file {}".format(
            args.filter_file
        ))


def read_json_string(string):
    try:
        return json.loads(string)
    except Exception as e:
        raise JFError(
            "Can't recognize filters from --filter argument: {}".format(
                args.filter)
        )


if __name__ == '__main__':
    """
    Data:
        Make sure data type is list. Flatten it if need.
        Make sure every element type is dict.
        Filter list of dicts by filters rules.
    Filters:
        Make sure filters type is dict.
        For every field get key as rule name and value as rule for field.
        Make filter with cascade lambdas for every rule
    """

    parser = argparse.ArgumentParser(description='''
    JSON filter with MongoDB shell syntax
    How to use:
    wget -qO - "http://rest.com/data" | jf -f \\
    '{"success": false, "errors": {"$size": 2}}'
    ''', formatter_class=RawTextHelpFormatter)
    parser.add_argument(
        '-f', '--filter', type=str, help='Allowed filters: {}'.format(
            ', '.join(expressions.keys())
        )
    )
    parser.add_argument(
        '-ff', '--filter-file', type=str, default='',
        help='File path to filters JSON'
    )
    parser.add_argument(
        '-k', '--key', type=str, default='',
        help='Array field name for incoming map-like JSON'
    )
    parser.add_argument(
        '-c', '--color', action='store_true',
        help='Colorize output (required pygments)'
    )
    args = parser.parse_args()

    try:
        filters = {}
        data = read_data_from_stdin()

        if args.filter_file:
            filters.update(load_data_from_file(args.filter_file))

        if args.filter:
            filters.update(read_json_string(args.filter))

        if args.key and type(data) == dict:
            data = get_values(args.key, data)

        if type(data) != list:
            raise JFError('Expected list, got {}'.format(type(data)))

        data = flatten(data)
        fset = make_filter_chain(data, filters)
        print(pretty_printable(fset, args.color))
        exit(0)

    except RegexpError:
        print(error_json("Wrong regex operation"))
    except (JFError, Exception) as e:
        print(error_json(str(e)))
    exit(1)
