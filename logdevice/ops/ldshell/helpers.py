#!/usr/bin/env python3

# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
import sys


def confirm_prompt(prompt, stream=sys.stdout):
    """Asks a question until it gets a y/n answer.  Returns bool."""
    return ask_prompt(prompt, options=["y", "n"], stream=stream) == "y"


def ask_prompt(prompt, options=None, help_str=None, stream=sys.stdout):
    options = [option.lower() for option in options]
    answer = ""
    help_suffix = "(type ? for help) " if help_str else ""

    print("")

    while answer.lower() not in options:
        answer = input("{} [{}] {}".format(prompt, "/".join(options), help_suffix))

        if answer == "?" and help_str:
            print(help_str)

    print("", file=stream)

    return answer.lower()
