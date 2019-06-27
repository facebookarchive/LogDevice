#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import textwrap
from collections import Counter

from nubia import context
from nubia.internal.cmdbase import Command
from prettytable import PrettyTable
from prompt_toolkit.completion import WordCompleter
from termcolor import cprint


def uniquify_names(names):
    """
    Takes a list of strings. If there are repeated values, appends some suffixes
    to make them unique.
    """
    count = {}
    for name in names:
        count.setdefault(name, 0)
        count[name] += 1

    seen = set()
    res = []
    for name in names:
        unique = count[name] == 1
        new_name = name
        i = 0
        # If a name is not unique, append suffix to all occurrences.
        # E.g. turn ["a", "a"] into ["a[0]", "a[1]"], not ["a", "a[0]"]
        while new_name in seen or (not unique and i == 0):
            new_name = name + "[" + str(i) + "]"
            i += 1
        seen.add(new_name)
        res.append(new_name)
    return res


# Output Printers


def _table_printer(headers, rows, column_sizes, delimiter):
    if not column_sizes:
        column_sizes = []
        for idx in range(len(headers)):
            column_sizes.append(len(headers[idx]))
        for row in rows:
            for idx in range(len(headers)):
                column_sizes[idx] = max(column_sizes[idx], len(row[idx]))

    colsfmt = [" {{:<{}}} ".format(s) for s in column_sizes]
    colsfmt.insert(0, "")
    colsfmt.append("")
    rowfmt = "|".join(colsfmt)
    headerline = rowfmt.format(*headers)
    hrule = "+{{:-<{}}}+".format(len(headerline) - 2).format("-")

    cprint(hrule)
    cprint(headerline)
    cprint(hrule)
    for row in rows:
        cprint(rowfmt.format(*row))
    cprint(hrule)


def _list_printer(headers, rows, _, delimiter):
    cprint(delimiter.join(headers))
    for row in rows:
        cprint(delimiter.join(row))


def _line_printer(headers, rows, *_):
    for row in rows:
        for hdr_idx in range(len(headers)):
            cprint("{} = {}".format(headers[hdr_idx], row[hdr_idx]))
        cprint("")


PRINTER_MAP = {"table": _table_printer, "list": _list_printer, "line": _line_printer}


class SelectCommand(Command):
    cmds = {
        "select": "a sql query interface for the tier, use `:tables` to get "
        "information about the tables available for query",
        "describe": "DESCRIBE <table name> to get detailed information "
        " about that table",
        ":last_query": "Gets detailed information on the last executed query",
        ":tables": "Shows a list of the supported tables",
        ":ttl": "Set cache ttl (0 to disable cache). Pro-tip: if you run the same "
        "query twice in a row, it will drop all caches before the second "
        "run",
        ":pretty": "Enable or disable pretty printing of LSNs and timestamps",
        ":server_side_filtering": "Enable or disable server-side-filtering",
        ":output_format": "One of: table, list, line. Determines the output format "
        "of ldquery results. Defaults to 'table'",
    }

    def __init__(self):
        super(Command, self).__init__()
        self._built_in = False
        self._cmds_map = {
            "select": self.run_select,
            "describe": self.run_describe,
            ":last_query": self.run_last_query,
            ":tables": self.run_tables,
            ":ttl": self.run_ttl,
            ":pretty": self.run_pretty,
            ":server_side_filtering": self.run_server_side_filtering,
            ":output_format": self.run_output_format,
        }
        self._last_res = None
        self._ldquery = None

        # add an empty autocompleter so that the completion thread doesn't
        # crash if we try to autocomplete before we connect
        self._table_completer_cached = None
        self._tables_cached = None
        self._prev_query = None
        self.parseable_output = False
        self.output_delimiter = "\t"
        self.running_from_cli = False
        self.output_format = "table"

    @property
    def ldquery(self):
        return context.get_context().ldquery

    @property
    def _tables(self):
        if not self.ldquery:
            return []
        if self._tables_cached is None:
            self._tables_cached = [t for t in self.ldquery.tables]
        return self._tables_cached

    @property
    def _table_completer(self):
        if self._table_completer_cached is None:
            self._table_completer_cached = WordCompleter(
                [t.name for t in self._tables], ignore_case=True
            )
        return self._table_completer_cached

    def get_completions(self, cmd, document, complete_event):
        if cmd.lower() == "describe" and self.ldquery:
            return self._table_completer.get_completions(document, complete_event)
        if cmd.lower() == "select":
            # Definitely not the best implementation of this, this is just a
            # hacky way to get basic select completion running, a better way is
            # to parse the sql statement using `sqlparse`
            # works if we are writing and from is the previous word
            elements = document.text_before_cursor.split()
            found = False
            if elements:
                if len(elements) > 1:
                    if elements[-1].lower() == "from":
                        found = True
                    if (
                        document.char_before_cursor != " "
                        and elements[-2].lower() == "from"
                    ):
                        found = True
            if found:
                return self._table_completer.get_completions(document, complete_event)
        return []

    def get_help(self, cmd, *args):
        return self.cmds[cmd]

    def get_command_names(self):
        return self.cmds.keys()

    def run_describe(self, cmd, input, raw):
        if not input:
            print("DESCRIBE <table_name>")
            return "Missing table name"
        for table in self._tables:
            if input.lower() == table.name:
                print()
                print("\n".join(textwrap.wrap(table.description, width=40)))
                pretty = PrettyTable(["Column", "Type", "Description"])
                pretty.align = "l"
                pretty.valign = "t"
                for c in table.columns:
                    pretty.add_row(
                        [
                            c.name,
                            c.type,
                            "\n".join(textwrap.wrap(c.description, width=40)),
                        ]
                    )
                print(str(pretty))
                return None
                break
        else:
            return "Unknown table `{}`".format(input)

    def run_last_query(self, cmd, input, raw):
        if input and input.strip().lower() == "details":
            if self._last_res is None:
                cprint("You need to run a query before running this command", "magenta")
                return "NO-QUERY"
            if not self._last_res.failed_nodes_count:
                print()
                cprint("No Failures", "cyan")
            else:
                print()
                cprint("Failures:", "magenta")
                failure_table = PrettyTable(["Address", "Failure"])
                for node_failure in self._last_res.failed_nodes:
                    failure_table.add_row(
                        [
                            node_failure.data().address,
                            node_failure.data().failure_reason,
                        ]
                    )
                cprint(str(failure_table))
        else:
            raise NotImplementedError("Not implemented yet!")

    def run_tables(self, cmd, input, raw):
        pretty = PrettyTable(["Table", "Description"])
        for table in self._tables:
            pretty.align = "l"
            pretty.add_row(
                [table.name, "\n".join(textwrap.wrap(table.description, width=40))]
            )
        print(str(pretty))

    def run_ttl(self, cmd, input, raw):
        if input is None or input == "":
            ttl = self.ldquery.cache_ttl
            cprint("Cache TTL is {} seconds".format(ttl))
        else:
            try:
                ttl = int(input)
            except ValueError:
                cprint("Usage: :ttl <number>")
                return
            self.ldquery.cache_ttl = ttl
            cprint("Cache TTL set to {} seconds".format(ttl))

    def run_pretty(self, cmd, input, raw):
        if input is None or input == "":
            cprint(
                "Pretty output is {}".format(
                    "on" if self.ldquery.pretty_output else "off"
                )
            )
            return
        if input in {"no", "off", "false", "0", "nope", "nah", "disable", "disabled"}:
            pretty = False
        elif input in {
            "yes",
            "on",
            "true",
            "1",
            "yep",
            "yeah",
            "totally",
            "enable",
            "enabled",
        }:
            pretty = True
        else:
            cprint("Usage: :pretty on|off")
            return
        self.ldquery.pretty_output = pretty
        cprint(
            "Pretty output set to {}".format(
                "on" if self.ldquery.pretty_output else "off"
            )
        )

    def run_server_side_filtering(self, cmd, input, raw):
        if input is None or input == "":
            cprint(
                "Server-side filtering is {}".format(
                    "on" if self.ldquery.server_side_filtering else "off"
                )
            )
            return
        if input in {"no", "off", "false", "0", "nope", "nah", "disable", "disabled"}:
            enabled = False
        elif input in {
            "yes",
            "on",
            "true",
            "1",
            "yep",
            "yeah",
            "totally",
            "enable",
            "enabled",
        }:
            enabled = True
        else:
            cprint("Usage: :server_side_filtering on|off")
            return
        self.ldquery.server_side_filtering = enabled
        cprint(
            "Server-side filtering is {}".format(
                "on" if self.ldquery.server_side_filtering else "off"
            )
        )

    def run_output_format(self, cmd, input, raw):
        if input is None or input == "":
            cprint("Output format is {}".format(self.output_format))
            return
        if input in PRINTER_MAP:
            self.output_format = input
        else:
            cprint("Usage: :output-format table|list|line", "red")
            return
        cprint("Output format is {}".format(self.output_format))

    def run_select(self, cmd, input, query):
        from logdevice.ldquery import LDQueryError, StatementError

        try:
            res = self.ldquery.execute_query(query)
            self._last_res = res
            print()
            column_names = uniquify_names(res.columns)
            if res.count:
                PRINTER_MAP[self.output_format](
                    column_names,
                    res._result.rows,
                    res._result.cols_max_size,
                    self.output_delimiter,
                )
            else:
                cprint("No records were retrieved.", "cyan")
            if res.failed_nodes_count:
                cprint("[WARNING] Incomplete results due to node(s) failures", "yellow")
                cprint(
                    "{}/{} nodes failed during that query".format(
                        res.failed_nodes_count, res.total_nodes_count
                    ),
                    "red",
                )

                per_failure_type = {}
                for n in res.failed_nodes:
                    reason = n.data().failure_reason
                    per_failure_type.setdefault(reason, [])
                    per_failure_type[reason].append(n.key())
                failure_table = PrettyTable(["Failure Reason", "Nodes"])
                for k, v in per_failure_type.items():
                    failure_table.add_row([k, ",".join(map(str, v))])
                print("--> Failure Stats")
                print(str(failure_table))

            print("{0:d} rows in set ({1:.2f} msec)".format(res.count, res.latency))

            return None
        except RuntimeError as e:
            print()
            cprint("[ERROR] A runtime error occured: {}".format(str(e)), "red")
            return "Invalid Statement"
        except StatementError as e:
            print()
            cprint("[ERROR] Invalid Statement: {}".format(str(e)), "red")
            return "Invalid Statement"
        except LDQueryError as e:
            print()
            cprint("[ERROR] Something went wrong in ldquery: {}".format(str(e)), "red")
            return "Unexpected Error"

    def run_interactive(self, cmd, input, query):
        if not self.ldquery:
            cprint(
                "You need to be connected to a logdevice cluster. You can use"
                " the 'connect' command to do so.",
                "red",
            )
            return 1
        if not self.running_from_cli and not self._prev_query:
            # enable pretty-printing by default
            self.ldquery.pretty_output = True
        drop_caches = query == self._prev_query
        if drop_caches:
            ttl_was = self.ldquery.cache_ttl
            self.ldquery.cache_ttl = 0
        try:
            errstr = self._cmds_map[cmd.lower()](cmd, input, query)
            if errstr:
                print(errstr)
            return 0
        finally:
            if drop_caches:
                self.ldquery.cache_ttl = ttl_was
            self._prev_query = query

    def run_cli(self, args):
        (cmd, rest) = args.query.split(" ", 1)
        if not args.nopretty:
            # Make sure LSNs are displayed with "eXnY" format instead of raw
            # integers.
            self.ldquery.pretty_output = True
        if args.output_format:
            if args.output_format not in PRINTER_MAP:
                cprint("Invalid --output-format '{}'".format(args.output_format))
                return 1
            self.output_format = args.output_format
        if args.disable_server_side_filtering:
            self.ldquery.server_side_filtering = False
        if args.delim:
            self.output_delimiter = args.delim
        self.running_from_cli = True
        return self.run_interactive(cmd, rest, args.query)

    def add_arguments(self, parser):
        subp = parser.add_parser("query", help=self.get_help("select"))
        subp.add_argument("query")
        subp.add_argument(
            "--nopretty",
            default=False,
            action="store_true",
            help=self.get_help(":pretty"),
        )
        subp.add_argument(
            "--output-format", default="table", help=self.get_help(":output_format")
        )
        subp.add_argument(
            "--disable-server-side-filtering", default=False, action="store_true"
        )
        subp.add_argument("--delim", default="\t")

    def get_cli_aliases(self):
        return ["query"]
