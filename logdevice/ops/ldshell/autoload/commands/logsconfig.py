#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import typing
from textwrap import dedent

from logdevice.client import (
    Directory,
    LogDeviceError,
    LogGroup,
    milliseconds_to_timestr,
    seconds_to_timestr,
    status as ErrorStatus,
    timestr_to_milliseconds,
    timestr_to_seconds,
)
from nubia import argument, command, context
from prompt_toolkit.shortcuts import confirm
from termcolor import cprint


def _ensure_connected():
    if not _get_client():
        cprint(
            "We cannot perform this operation if ldshell is not connected "
            "to a logdevice cluster!"
        )
        raise Exception("Not connected to a logdevice cluster!")


_attributes = [
    argument(
        "replication_factor",
        type=int,
        description=dedent(
            """
                Number of nodes on which to persist a record.
                Optional if replicate_across is present.
            """
        ),
    ),
    argument(
        "extra_copies",
        type=int,
        description=dedent(
            """
                Number of extra copies the sequencer sends out to storage nodes
                ('x' in the design doc).  If x > 0, this is done to improve
                latency and availability; the sequencer will try to delete extra
                copies after the write is finalized.
             """
        ),
    ),
    argument(
        "synced_copies",
        type=int,
        description=dedent(
            """
                The number of copies that must be acknowledged by storage nodes
                as synced to disk before the record is acknowledged to client as
                fully appended. Can be 0. Capped at replicationFactor.
                 """
        ),
    ),
    argument(
        "max_writes_in_flight",
        type=int,
        description=dedent(
            """
                The largest number of records not released for delivery that the
                sequencer allows to be outstanding ('z' in the design doc).
             """
        ),
    ),
    argument(
        "single_writer",
        type=int,
        description=dedent(
            """
                Does LogDevice assume that there is a single writer for the log?
             """
        ),
    ),
    argument(
        "sync_replicate_across",
        type=str,
        description=dedent(
            """
                The location scope to enforce failure domain properties, by default
                the scope is in the individual node level.
                "replicate-across" provides a more general way to do the same thing.
             """
        ),
    ),
    argument(
        "replicate_across",
        type=typing.Mapping[str, int],
        description=dedent(
            """
                Defines cross-domain replication. A vector of replication factors
                at various scopes. When this option is given, replicationFactor_ is
                optional. This option is best explained by examples:
                 - "node: 3, rack: 2" means "replicate each record to at least 3 nodes
                   in at least 2 different racks".
                 - "rack: 2" with replicationFactor_ = 3 mean the same thing.
                 - "rack: 3, region: 2" with replicationFactor_ = 4 mean "replicate
                   each record to at least 4 nodes in at least 3 different racks in at
                   least 2 different regions"
                 - "rack: 3" means "replicate each record to at least 3 nodes in
                   at least 3 different racks".
                 - "rack: 3" with replicationFactor_ = 3 means the same thing.
             """
        ),
    ),
    argument(
        "nodeset_size",
        type=int,
        description=dedent(
            """
               Size of the nodeset for the log. Optional. If value is not specified,
               the nodeset for the log is considered to be all storage nodes in the
               config.
             """
        ),
    ),
    argument(
        "backlog",
        type=str,
        description=dedent(
            """
               Duration (in seconds) that a record can exist in the log before
               it expires and gets deleted. Valid value must be at least 1 second.
             """
        ),
    ),
    argument(
        "delivery_latency",
        type=str,
        description=dedent(
            """
               Maximum amount of time to artificially delay
               delivery of newly written
               records (increases delivery latency but improves server and client
               performance).
             """
        ),
    ),
    argument(
        "scd_enabled",
        type=bool,
        description=dedent(
            """
               Indicate whether or not the Single Copy Delivery optimization should be
               used.
             """
        ),
    ),
    argument(
        "local_scd_enabled",
        type=bool,
        description=dedent(
            """
               Indicate whether or not the Local Single Copy Delivery optimization
               should be used. This prioritizes the reader's copies within a region
               instead of cross regional reads.
             """
        ),
    ),
    argument(
        "write_token",
        type=str,
        description=dedent(
            """
               If this is nonempty, writes to the log group are only allowed if
               Client::addWriteToken() was called with this string.
             """
        ),
    ),
    argument(
        "sticky_copysets",
        type=bool,
        description=dedent(
            """
               True if copysets on this log should be "sticky". See docblock in
               StickyCopySetManager.h
             """
        ),
    ),
    argument(
        "mutable_per_epoch_log_metadata_enabled",
        type=bool,
        description=dedent(
            """
               If true, write mutable per-epoch metadata along with every data record.
             """
        ),
    ),
    argument(
        "permissions",
        type=typing.Mapping[str, typing.List[str]],
        description=dedent(
            """
               Maps a principal to a set of permissions. Used by ConfigPermissionChecker
               and is populated when the permission_checker_type in the conifg file is
               set to 'config'
             """
        ),
    ),
    argument(
        "sequencer_batching",
        type=bool,
        description=dedent(
            """
               Accumulate appends from clients and batch them together to
               create fewer records in the system.
             """
        ),
    ),
    argument(
        "sequencer_batching_size_trigger",
        type=int,
        description=dedent(
            """
               Sequencer batching (if used) flushes buffered appends for a log when
               the
               total amount of buffered uncompressed data reaches this many bytes (if
               positive).
             """
        ),
    ),
    argument(
        "sequencer_batching_compression",
        type=str,
        description=dedent(
            """
               Compression setting for sequencer batching (if used). It can be
               'none' for no compression; 'zstd' for ZSTD; 'lz4' for LZ4; or lz4_hc for
               LZ4 High Compression. The default is ZSTD.
             """
        ),
    ),
    argument(
        "sequencer_batching_passthru_threshold",
        type=int,
        description=dedent(
            """
               Sequencer batching (if used) will pass through any appends with payload
               size over this threshold (if positive).  This saves us a compression
               round trip when a large batch comes in from BufferedWriter and the
               benefit of batching and recompressing would be small.
             """
        ),
    ),
    argument(
        "sequencer_batching_time_trigger",
        type=int,
        description=dedent(
            """
                Sequencer batching (if used) will flush the buffered appends if
                the oldest buffered append is this old (in milliseconds)
            """
        ),
    ),
    argument(
        "extra_attributes",
        type=typing.Mapping[str, str],
        aliases=["extras"],
        description=dedent(
            """
               Arbitrary fields that logdevice does not recognize
             """
        ),
    ),
    argument(
        "shadow",
        type=typing.Mapping[str, str],
        description=dedent(
            """
               Parameters for traffic shadowing. Must be specified as a
               dictionary that contains both 'destination', which is the config
               file url for the destination shadow cluster, and 'ratio', the
               percentage of logs to shadow, in the range [0.0, 1.0]. Example:
                 --shadow "destination: file:cluster.conf; ratio: 0.1"
               Shadowing can be disabled by unsetting this attribute.
             """
        ),
    ),
]


def _get_client():
    return context.get_context().get_client()


def _clone_attributes(attrs):
    return {k: v for k, v in attrs.items()}


def _print_attributes(attrs, indentation=0):
    space_shift = " " * indentation
    for k, v in attrs.items():
        cprint(space_shift + "  - {}: ".format(k.replace("_", "-")), end="")
        if "is_edited" in v and v["is_edited"]:
            value_color = "magenta"
        else:
            value_color = None

        # special handling for backlog and delivery_latency
        if k == "backlog" and v["value"] is not None:
            value = seconds_to_timestr(v["value"])
        elif k == "delivery_latency" and v["value"] is not None:
            value = milliseconds_to_timestr(v["value"])
        else:
            value = v["value"]
        cprint("{}".format(value), value_color, end="")
        if not v["is_inherited"]:
            if "is_edited" in v and v["is_edited"]:
                cprint(" (Edited)", "yellow", end="")
            else:
                cprint("    (Overridden)", "white", end="")
        print()


def _print_log_group_helper(lg, indentation=0, show_version=False):
    space_shift = " " * indentation
    cprint(
        space_shift
        + "\u25CE "
        + str(lg)
        + " ({}..{})".format(lg.range[0], lg.range[1]),
        "yellow",
    )
    if show_version:
        cprint(space_shift + "  Version: {}".format(lg.version))
    cprint(space_shift + "  Attributes:", "white")
    _print_attributes(lg.attrs, indentation)


def _print_directory_helper(
    level, directory, max_depth, print_log_groups, show_version=False
):
    if level > max_depth:
        return
    shift = 4
    indentation = level * shift
    next_level = level + 1
    space_shift = " " * indentation
    if level == max_depth:
        cprint(
            space_shift
            + "\u25B6\uFE0E "
            + directory.fully_qualified_name
            + " [{} log groups, {} children folded]".format(
                len(directory.logs), len(directory.children)
            ),
            "blue",
        )
    else:
        cprint(space_shift + "\u25BC " + directory.fully_qualified_name, "green")
    if show_version:
        cprint(space_shift + "  Version: {}".format(directory.version))
    cprint(space_shift + "  Attributes: ", "white")
    _print_attributes(directory.attrs, indentation)
    # print log groups if asked for
    if level < max_depth:
        if print_log_groups and len(directory.logs) > 0:
            for _, loggroup in directory.logs.items():
                _print_log_group_helper(loggroup, next_level * shift, show_version)
                print()
        for _, child in directory.children.items():
            _print_directory_helper(
                next_level, child, max_depth, print_log_groups, show_version
            )
            print()


def _print_log_group(lg, show_version=False):
    """Prints information about log group defined for this path"""
    return _print_log_group_helper(lg, 0, show_version=show_version)


def _print_directory(directory, max_depth=1, log_groups=True, show_version=False):
    """Prints information about directory defined for this path"""
    return _print_directory_helper(0, directory, max_depth, log_groups, show_version)


def _get_logsconfig_node(path=None, logid=None):
    """
    Returns a node in the tree defined at the supplied 'path'. This can be a
    directory or a log-group.

    @throws KeyError if the path does not exist
    """
    try:
        c = _get_client()
        if logid is not None:
            # We use the logid and ignore the path in this case. logid is very
            # specific and should either result
            return c.get_log_group_by_id(logid)
        else:
            path = c.get_directory_delimiter() if path is None else path
        return c.get_log_group_by_name(str(path))
    except LogDeviceError:
        try:
            return c.get_directory(str(path))
        except LogDeviceError:
            raise KeyError("Path {} is not found!".format(str(path)))


def _update_attributes_time_string(attrs):
    for k, v in attrs.items():
        if k == "backlog" and v is not None:
            # we expect the value to be string
            assert isinstance(v, str)
            try:
                attrs[k] = timestr_to_seconds(v)
            except ValueError as e:
                cprint("backlog: {}".format(e), "red")
                return None
        elif k == "delivery_latency" and v is not None:
            # we expect the value to be string
            assert isinstance(v, str)
            try:
                attrs[k] = timestr_to_milliseconds(v)
            except ValueError as e:
                cprint("delivery-latency: {}".format(e), "red")
                return None
    return attrs


def _update_shadow_params(attrs):
    if "shadow" in attrs:
        shadow = attrs["shadow"]
        if "destination" not in shadow or "ratio" not in shadow:
            cprint(
                "shadow: must contain both destination and ratio "
                "(separated by semicolon)",
                "red",
            )
            return None
        try:
            ratio = float(shadow["ratio"])
        except ValueError as e:
            cprint("shadow ratio: {}".format(e), "red")
            return None
        if ratio < 0.0 or ratio > 1.0:
            cprint("shadow ratio: must be in range [0.0, 1.0]", "red")
            return None
        shadow["ratio"] = ratio
        attrs["shadow"] = shadow
    return attrs


def _wrap_with_attributes(command):
    for decorator in _attributes:
        decorator(command)


@command
class Logs:
    """
    Control the logs config of logdevice dynamically. This allows you to create
    or delete log groups and directories (groups of log groups).
    """

    @command
    @argument(
        "path", description="Path of the log group to be created.", positional=True
    )
    @argument("from_id", name="from", description="The beginning of the logid range")
    @argument("to_id", name="to", description="The end of the logid range")
    @argument(
        "is_directory",
        name="directory",
        description="Whether we should create a directory instead",
    )
    @argument(
        "show_version",
        description="Should we show the version of the config tree after "
        "the operation or not",
    )
    def create(
        self,
        path: str,
        from_id: typing.Optional[int] = None,
        to_id: typing.Optional[int] = None,
        is_directory: bool = False,
        show_version: bool = True,
        **kwargs
    ):
        """
        Creates a log group under a specific directory path in the LogsConfig tree.
        This only works if the tier has LogsConfigManager enabled.
        """
        _ensure_connected()
        # Validate arguments
        if is_directory and (from_id or to_id):
            cprint(
                "You cannot have be creating a directory and setting from/to"
                " log range at the same time.",
                "red",
            )
            return 1
        if not is_directory and (from_id is None or to_id is None):
            cprint(
                "'from' and 'to' must be set for creating log-groups. If you"
                " are trying to create a directory, use --directory"
            )
            return 1
        attributes = {k: v for k, v in kwargs.items() if v is not None}
        # convert backlog and delivery_latency
        attributes = _update_attributes_time_string(attributes)
        attributes = _update_shadow_params(attributes)
        if attributes is None:
            return 5

        try:
            c = _get_client()
            if is_directory:
                d = c.make_directory(str(path), True, attributes)
                _print_directory_helper(
                    0, d, max_depth=1, print_log_groups=False, show_version=show_version
                )
            else:
                lg = c.make_log_group(str(path), from_id, to_id, attributes, True)
                _print_log_group_helper(lg, show_version=show_version)
        except LogDeviceError as e:
            cprint(
                "Could not create '{}'. {}: {}".format(path, e.args[1], e.args[3]),
                "red",
            )

            return 1

    # This adds the list of attributes as decorators to this method
    _wrap_with_attributes(create)

    @command
    @argument(
        "path",
        description="The path you want to print, if missing this prints the "
        "full tree",
    )
    @argument("logid", name="id", description="Only the log-group that has this ID")
    @argument("max_depth", description="How many levels in the tree you want to see?")
    def show(
        self,
        path: typing.Optional[str] = None,
        logid: typing.Optional[int] = None,
        max_depth: int = 1000,
    ):
        """
        Prints the full logsconfig for this tier
        """
        _ensure_connected()
        try:
            node = _get_logsconfig_node(path, logid)
        except KeyError as e:
            cprint("Error: {}".format(e), "red")
            return 1

        if isinstance(node, Directory):
            _print_directory(
                node, max_depth=max_depth, log_groups=True, show_version=True
            )
        elif isinstance(node, LogGroup):
            _print_log_group(node, show_version=True)
        else:
            # I don't know what that is!
            cprint(
                "Error: TypeError, _get_logsconfig_node() returned unknown type!", "red"
            )
            return 2

    @command
    @argument(
        "old_path",
        description="The original path of the node you want to rename",
        positional=True,
    )
    @argument("new_path", description="The new path of the node", positional=True)
    def rename(self, old_path: str, new_path: str):
        """Renames a path in logs config to a new path, this cannot move nodes
        In the tree, so only the leaf of the path is allowed to be renamed"""

        _ensure_connected()
        try:
            c = _get_client()
            if not context.get_context().args.yes and not confirm(
                "Are you sure you want to rename "
                '"{}" to "{}"? (y/n)'.format(old_path, new_path)
            ):
                return
            version = c.rename(str(old_path), str(new_path))
            cprint(
                "Path '{}' has been renamed to '{}' in version {}".format(
                    old_path, new_path, version
                )
            )

        except LogDeviceError as e:
            cprint("Cannot perform rename. {}: {}".format(e.args[1], e.args[3]), "red")

        return 1

    @command
    @argument(
        "path",
        type=str,
        description="Path of the node you want to set attributes for",
        positional=True,
    )
    @argument(
        "unset",
        type=typing.List[str],
        description="The list of attribute names to unset",
    )
    def update(self, path, unset=None, **kwargs):
        """
        This updates the LogAttributes for the node (can be either a directory
        or a log group) under a specific directory path in the LogsConfig tree.
        This will *merge* the currently set attributes with
        the supplied attributes. You can unset specific attributes by using the
        'unset' argument that takes a list of attribute names to unset.
        This only works if the tier has LogsConfigManager enabled.
        """

        attributes = {k: v for k, v in kwargs.items() if v is not None}
        # convert backlog and delivery_latency
        attributes = _update_attributes_time_string(attributes)
        attributes = _update_shadow_params(attributes)
        if attributes is None:
            return 5
        unset = unset or []
        edits = 0
        try:
            node = _get_logsconfig_node(path)
            current_attributes = node.attrs
        except KeyError as e:
            cprint("Error: {}".format(e), "red")
            return 1

        # Are we unsetting some attributes?
        if unset:
            # let's first replace '-' with '_' (visual => dict repr)
            unset = [key.replace("-", "_") for key in unset]

        # We need to calculate the new attributes based on the current attributes
        # let's update the attributes object with all attributes that are set in the
        # current attributes but not set in kwargs.
        new_attributes = _clone_attributes(current_attributes)
        for attr_key, attr_value in attributes.items():
            new_value = {"value": attr_value, "is_inherited": False}
            # do we really need to set it? is it already set (and overridden)
            if attr_key not in new_attributes or new_attributes[attr_key] != new_value:
                new_value["is_edited"] = True
                new_attributes[attr_key] = new_value
                edits += 1

        for k in unset:
            if kwargs.get(k) is not None:
                # We cannot set and unset the value at the same time!
                cprint(
                    "You cannot set attribute '{}' and unset it at the same"
                    " time!".format(k),
                    "red",
                )
                return 3
            # do we really need to unset it? is it already unset (and overridden)
            if k not in current_attributes:
                # We don't know anything about this attribute!
                cprint(
                    "Unknown attribute '{}' that you are trying to " "unset!".format(k),
                    "red",
                )
                return 1

            if current_attributes[k]["is_inherited"] is False:
                # When we are unsetting, we don't know the parent value that will be
                # inherited after unsetting this key. So, we show None in this case.
                new_attributes[k] = {
                    "value": None,
                    "is_inherited": False,
                    "is_edited": True,
                }
                edits += 1

        if not edits:
            # No edits were made, print the current attributes and let the user know
            cprint("Your updates did not result in changes in the attributes", "green")
            cprint("Current Attributes: ", "yellow")
            _print_attributes(current_attributes)
            # We don't consider this a failure as the desired state is already
            # applied
            return 0

        cprint("Original Attributes: ", "yellow")
        _print_attributes(current_attributes)
        print()

        cprint("New Attributes will be: ", "magenta")
        _print_attributes(new_attributes)

        effective_attributes_to_apply = {
            k: v["value"]
            for k, v in new_attributes.items()
            if v["is_inherited"] is False and v["value"] is not None
        }

        # Print a warning if changing backlog duration (S152007)
        ctx = context.get_context()
        try:
            if not ctx.args.yes and not confirm(
                "Are you sure you want to update the attributes at '{}'? (y/n)".format(
                    path
                )
            ):
                return
            c = _get_client()
            version = c.set_attributes(str(path), effective_attributes_to_apply)
            cprint(
                "Attributes for '{}' has been updated in version {}!".format(
                    path, version
                )
            )

        except LogDeviceError as e:
            cprint(
                "Cannot update attributes for '{}'. {}: {}".format(
                    path, e.args[1], e.args[3]
                ),
                "red",
            )

            return 1

    # This adds the list of attributes as decorators to this method
    _wrap_with_attributes(update)

    @command
    @argument("path", type=str, description="Path of the log group", positional=True)
    @argument("from_id", name="from", description="The beginning of the logid range ")
    @argument("to_id", name="to", description="The end of the logid range")
    def set_range(self, path: str, from_id: int, to_id: int):
        """
        This updates the log id range for the LogGroup under
        a specific directory path in the LogsConfig tree.
        This only works if the tier has LogsConfigManager enabled.
        """
        try:
            c = _get_client()
            current_log_group = c.get_log_group_by_name(str(path))
            if not context.get_context().args.yes and not confirm(
                "Are you sure you want to set"
                " the log range at "
                '"{}" to be ({}..{}) instead of({}..{})? (y/n)'.format(
                    path,
                    from_id,
                    to_id,
                    current_log_group.range[0],
                    current_log_group.range[1],
                )
            ):
                return
            version = c.set_log_group_range(str(path), from_id, to_id)
            cprint(
                "Log group '{}' has been updated in version {}!".format(path, version)
            )

        except LogDeviceError as e:
            cprint("Cannot update range for '{}': {}".format(path, e), "red")

    @command
    @argument(
        "path", description="Path of the directory to be removed.", positional=True
    )
    @argument(
        "recursive",
        description="Whether to remove the contents of the directory if it "
        "is not empty or not",
    )
    def remove(self, path: str, recursive: bool = False):
        """
        Removes a directory or a log-group under a specific directory path in
        the LogsConfig tree. This will NOT delete the directory if it is not
        empty by default, you need to use --recursive (recursive=True).
        """

        try:
            c = _get_client()
            if not context.get_context().args.yes and not confirm(
                "Are you sure you want to REMOVE " "'{}'? (y/n)".format(path)
            ):
                return
            try:
                version = c.remove_log_group(str(path))
            except LogDeviceError as e:
                if e.args[0] == ErrorStatus.NOTFOUND:
                    version = c.remove_directory(str(path), recursive)
            cprint("'{}' has been removed in version {}".format(path, version))

        except LogDeviceError as e:
            cprint("Cannot remove '{}'. Reason: {}".format(path, e.args[2]), "red")

            return 1
