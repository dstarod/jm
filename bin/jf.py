#!/usr/bin/env python3

import json
import sys
import argparse
import re
from argparse import RawTextHelpFormatter

enabled_colors = False
try:
    from pygments import highlight
    from pygments.lexers import JsonLexer
    from pygments.formatters import TerminalFormatter
    enabled_colors = True
except ImportError:
    pass

# TODO https://docs.mongodb.com/v3.0/reference/operator/query/
# Element: $type
# Evaluation: $mod, $text, $where
# Geospatial: $geoWithin, $geoIntersects, $near, $nearSphere
# Array: $all
# Comments?
# Projection Operators?

NOT_FOUND = '$FiElD%N0T!F0uND&'

# Regex cache
regexps = {}


class JFError(Exception):
    pass


class RegexpError(JFError):
    pass


def exp_regexp(search_string, regex_string):
    """
    Regex filter function
    :param search_string: str
    :param regex_string: str
    :return: bool
    """
    if search_string is NOT_FOUND:
        return False

    try:
        if regex_string not in regexps:
            regexps[regex_string] = re.compile(regex_string)
        return re.match(regexps[regex_string], str(search_string))
    except Exception:
        raise RegexpError


def elem_match(values, filter_expr):
    """
    Element match filter function
    :param values: list - values
    :param filter_expr: lambda function
    :return: bool
    """
    for val in values:
        if filter_expr(val):
            return True

    return False


expressions = {
    # Comparison
    "$gt": lambda x, y: x is not NOT_FOUND and x > y,
    "$gte": lambda x, y: x is not NOT_FOUND and x >= y,
    "$eq": lambda x, y: x is not NOT_FOUND and x == y,
    "$lt": lambda x, y: x is not NOT_FOUND and x < y,
    "$lte": lambda x, y: x is not NOT_FOUND and x <= y,
    "$ne": lambda x, y: x is not NOT_FOUND and x != y,
    "$in": lambda x, y: x is not NOT_FOUND and (set(x) & set(y) if type(x) == list else x in y),
    "$nin": lambda x, y: x is not NOT_FOUND and (not set(x) & set(y) if type(x) == list else x not in y),
    # Element
    "$exists": lambda x, y: x is not NOT_FOUND if y else x is NOT_FOUND,
    # Evaluation
    "$regex": exp_regexp,
    # Array
    "$size": lambda x, y: x is not NOT_FOUND and len(x) == y,
    "$elemMatch": elem_match
}


def get_values(path, data):
    """
    Returns all available values by complex path
    :param path: str - complex path, like "jf.tool" -> {"jf": {"tool": true}}
    :param data: dict
    :return: list - values
    """
    if type(data) != dict:
        raise JFError("Expected dict, got {}".format(type(data)))

    step = data
    path_parts = path.split('.')
    for path_num, path_part in enumerate(path_parts):
        if path_part not in step:
            # Next path part not found
            return []

        step = step[path_part]

        if type(step) == list:
            if path_num < len(path_parts)-1:
                next_path_parts = '.'.join(path_parts[path_num+1:])
                ret = []
                for part_data in step:
                    if type(part_data) != dict:
                        continue
                    ret.extend(get_values(next_path_parts, part_data))
                return ret
            return step

        if type(step) != dict:
            if path_num < len(path_parts)-1:
                # Found path shorter than expected
                return []
            else:
                return [step]


def gen_lambda(filter_key, filter_value, exp_name='$eq'):
    """
    Generate lambdas set for filters rules
    :param filter_key: str
    :param filter_value: str or dict
    :param exp_name: str - key of the expressions dict
    :return: lambda function
    """

    exp = expressions.get(exp_name, expressions['$eq'])

    if filter_key == '$not' and type(filter_value) == dict:
        return lambda x: not any([
            gen_lambda(fname, frule)(x)
            for fname, frule in filter_value.items()
        ])

    if type(filter_value) == dict:
        return lambda x: all([
            gen_lambda(filter_key, filter_rule, exp_name)(x)
            for exp_name, filter_rule in filter_value.items()
        ])

    if filter_key == '$and' and type(filter_value) == list:
        return lambda x: all([
            gen_lambda(k, v)(x)
            for f in filter_value for k, v in f.items()
        ])

    if filter_key == '$or' and type(filter_value) == list:
        return lambda x: any([
            all([
                gen_lambda(k, v)(x)
                for k, v in f.items()  # all parts of the dict must be true
            ])
            for f in filter_value  # for every item in the or-list
        ])

    if filter_key == '$nor' and type(filter_value) == list:
        return lambda x: not any([
            all([
                gen_lambda(k, v)(x)
                for k, v in f.items()  # all parts of the dict must be true
            ])
            for f in filter_value  # for every item in the or-list
        ])

    if filter_key == '$elemMatch' and type(filter_value) == dict:
        def filter_func(x): return all([
            gen_lambda(k, v)(x)
            for k, v in filter_value.items()
        ])
        return lambda x: exp(x, filter_func)

    return lambda x: any([
       exp(val, filter_value) for val in get_values(filter_key, x)
    ])


def make_filter_chain(data, filters):
    """
    Create filter chain based on initial data and filters
    :param data: list - initial data
    :param filters: dict - filters
    :return: filter
    """
    return filter(
        lambda x: all([
            gen_lambda(field_name, filter_value)(x)
            for field_name, filter_value in filters.items()
        ]),
        data
    )


def flatten(items, empty=[]):
    """
    Flat nested arrays
    :param items: list - nested list
    :param empty: list - flat list
    :return: list - flat list
    """
    for i in items:
        if type(i) == list:
            flatten(i, empty)
        else:
            empty.append(i)
    return empty


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
