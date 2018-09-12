#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
from __future__ import absolute_import, division, print_function, unicode_literals

import sys

from logdevice.ldquery.internal import ext


DEFAULT_TIMEOUT = 10


class LDQuery(object):
    def __init__(self, config_path, timeout=DEFAULT_TIMEOUT, use_ssl=False):
        self._client = ext.LDQueryBinding(config_path, timeout, use_ssl)
        self._tables = None

    @property
    def pretty_output(self):
        """
        If val is true, LSN and timestamp values are returned in their human
        readable format instead of raw integers.
        """
        return self._client.get_pretty_output()

    @pretty_output.setter
    def pretty_output(self, val):
        self._client.set_pretty_output(val)

    @property
    def cache_ttl(self):
        return self._client.get_cache_ttl()

    @cache_ttl.setter
    def cache_ttl(self, seconds):
        self._client.set_cache_ttl(seconds)

    @property
    def server_side_filtering(self):
        return self._client.server_side_filtering_enabled()

    @server_side_filtering.setter
    def server_side_filtering(self, val):
        self._client.enable_server_side_filtering(val)

    def execute(self, statement):
        """
        Runs the query string (can be multiple queries separated by semi-column)
        and returns a list of Cursors corresponding to the inlined queries in
        the string. Usually a single query is passed so you will need to extract
        the first element from the result to get the Cursor object

        result = l.execute("select * from info; select * from stats")[0]
        """
        return map(Cursor, self._client.query(statement))

    def execute_query(self, statement):
        """
        Runs the passed query (must be a single statement) and returns a Cursor
        object, if you want to execute multiple query, use execute() instead.

        cursor = l.execute_query('select * from info')
        """
        results = self._client.query(statement)
        if not results:
            return None
        return Cursor(results[0])

    @property
    def tables(self):
        if not self._tables:
            self._tables = self._client.get_tables()
        return self._tables


class Row(object):
    def __init__(self, raw_row, cursor):
        self._cursor = cursor
        self._row = raw_row

    def get(self, header):
        return self._row[self._cursor.getIndex(header)]

    def __iter__(self):
        for pos, header in enumerate(self._cursor.headers):
            yield header, self._row[pos]

    def __getattr__(self, attr):
        return self.get(attr)

    def __getitem__(self, key):
        if type(key) is int:
            return self._row[key]

        return self.get(key)


class Cursor(object):
    """
    A Cursor object that offers an interface for a result-set returned from a
    set of nodes, this is a wrapper over the native c++ bindings of ldquery.
    """

    def __init__(self, result):
        self._result = result
        self._indexes = {}
        for i, header in enumerate(self._result.headers):
            self._indexes[header] = i

    def __iter__(self):
        for row in self._result.rows:
            yield Row(row, self)

    def getIndex(self, header):
        return self._indexes[header]

    @property
    def headers(self):
        return self._result.headers

    @property
    def count(self):
        """
        The total number of results returned.
        """
        return self._result.rows.size

    def __len__(self):
        return self.count

    @property
    def complete(self):
        """
        Returns True if the result is not-partial due to trimming or
        failures on the server side.
        """
        return self._result.metadata.success

    @property
    def total_nodes_count(self):
        """
        The number of nodes contacted during this operation
        """
        return self._result.metadata.contacted_nodes

    @property
    def failed_nodes_count(self):
        """
        The number of failed nodes during this query
        """
        return self._result.metadata.failures.size

    @property
    def failed_nodes(self):
        """
        Returns an object holding details about the failures per node
        """
        return self._result.metadata.failures

    @property
    def latency(self):
        """
        The total time it took to run this query in milliseconds
        """
        return self._result.metadata.latency

    @property
    def columns(self):
        """
        A list of the columns returned in this result-set
        """
        return list(self._result.headers)
