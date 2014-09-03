# -*- coding: utf-8 -*-


import os
import sys
import json

sys.path.insert(0, '..')
from thriftpy import parse


def _json(name):
    path = os.path.join('parser-cases', 'json', name + '.json')
    return json.load(open(path))


def _thrift(name):
    path = os.path.join('parser-cases', 'thrift', name + '.thrift')
    return parse(open(path).read())


class TestParser(object):

    def case(name):
        def _case(self):
            print _thrift(name)
            assert _thrift(name) == _json(name)
        return _case

    test_includes = case('includes')
    test_namespaces = case('namespaces')
    test_comments = case('comments')
    test_consts = case('consts')
    test_typedefs = case('typedefs')
    test_enums = case('enums')
