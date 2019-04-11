#!/usr/bin/env python
# Copyright (c) Facebook, Inc. and its affiliates.
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
'''

Extends FBCodeBuilder to emit a Legocastle "steps".

Unlike DockerFBCodeBuilder, which either checks out a Github repository, or
copies a local one, this builder is designed to get code for projects that
use `fb_github_project_workdir` from the local fbsource repository.

See `facebook_fbsource_utils.py` and `facebook_make_legocastle_job.py` for
the details of how Github-analogous code is obtained.

This file simply enumerates the project needing this special treatment, and
emits a sequence of bash-command steps that expect the projects to be on
disk (provided via a Legocastle bundle).

'''

import itertools


from collections import namedtuple

from fbcode_builder import FBCodeBuilder
from shell_quoting import (
    path_join, raw_shell, shell_comment, shell_join, ShellQuoted
)
from utils import recursively_flatten_list


class LegocastleStepError(RuntimeError):
    def __init__(self, message):
        super(LegocastleStepError, self).__init__(
            '{0}. Since I am not sure of the best way to translate this to '
            'flat Legocastle steps, I am giving up for now. Ask lesha@ if '
            'this seems like an essential feature.'.format(message)
        )


LegocastleFBCheckout = namedtuple('LegocastleFBCheckout', ['project_and_path'])
LegocastleWorkdir = namedtuple('LegocastleWorkdir', ['dir'])


class LegocastleStep(namedtuple('LegocastleStepBase', ('name', 'actions'))):
    def __new__(cls, name, actions):
        flat_actions = list(recursively_flatten_list(actions))
        for action in flat_actions:
            if isinstance(action, LegocastleStep):
                raise LegocastleStepError(
                    'Your config has a step() nested inside a step()'
                )
        return super(LegocastleStep, cls).__new__(cls, name, flat_actions)


class LegocastleFBCodeBuilder(FBCodeBuilder):

    def setup(self):
        return self.step('Setup', [
            self.create_python_venv()] + [
            self.run(ShellQuoted("""
case "$OSTYPE" in
  darwin*)
    http_proxy= https_proxy= ./tools/lfs/lfs.py \\
            download homebrew.tar.gz \\
            -l ./watchman/facebook/lego/.lfs-pointers
    rm -rf /var/tmp/homebrew
    tar xzf homebrew.tar.gz -C /var/tmp
  ;;
esac
"""))])

    def step(self, name, actions):
        return LegocastleStep(name=name, actions=actions)

    def run(self, shell_cmd):
        return shell_cmd

    def workdir(self, dir):
        return LegocastleWorkdir(dir)

    def fb_github_project_workdir(self, project_and_path, github_org='facebook'):
        return LegocastleFBCheckout(project_and_path)

    def comment(self, comment):
        # Sandcastle can't deal with many single quotes (#15546368) so
        # we can't `echo` the comment or anything like that.
        return shell_comment(comment)

    def _render_impl(self, actions):
        next_workdir = None
        shipit_projects = []
        build_steps = []
        for action in recursively_flatten_list(actions):
            if isinstance(action, LegocastleFBCheckout):
                next_workdir = ShellQuoted(
                    '"$(hg root)/"{d}',
                ).format(
                    d=path_join(
                        self.option('shipit_project_dir'),
                        action.project_and_path
                    ),
                )
                shipit_projects.append(
                    action.project_and_path.split('/', 1)[0]
                )
            elif isinstance(action, LegocastleStep):
                pre_actions = [
                    ShellQuoted('set -ex')
                ]
                if action.name != 'Setup' and \
                        self.option("PYTHON_VENV", "OFF") == "ON":
                    pre_actions.append(self.python_venv())
                pre_actions.append(
                    ShellQuoted("""
case "$OSTYPE" in
  darwin*)
    BREW_PREFIX=/var/tmp/homebrew

    # The apple-provided flex and bison tools are too old to successfully
    # build thrift.  Ensure that we resolve to the homebrew versions.
    # Note that homebrew doesn't link these into its bin dir to avoid
    # these newer versions taking precedence, so we need to reach into
    # the cellar path.  The glob is to make this script less prone to
    # problems if/when the version is bumped.
    BISON_BIN=$(echo $BREW_PREFIX/Cellar/bison/*/bin)
    FLEX_BIN=$(echo $BREW_PREFIX/Cellar/flex/*/bin)

    export CMAKE_SYSTEM_PREFIX_PATH=$BREW_PREFIX
    export PKG_CONFIG_PATH=$BREW_PREFIX/opt/openssl/lib/pkgconfig
    export PATH=$BISON_BIN:$FLEX_BIN:$BREW_PREFIX/bin:$PATH
    export HOMEBREW_NO_AUTO_UPDATE=1
    export SDKROOT=/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk/
  ;;
esac
""")
                )
                if next_workdir is not None:
                    pre_actions.append(self.workdir(next_workdir))

                next_workdir = None
                shell_steps = []
                for a in itertools.chain(pre_actions, action.actions):
                    if isinstance(a, LegocastleWorkdir):
                        # Pass the last working directory to the next step,
                        # since Legocastle doesn't remember it.
                        next_workdir = a.dir
                        shell_steps.append(
                            ShellQuoted(
                                # Don't evaluate {d} twice.
                                '_wd={d} ; mkdir -p "$_wd" && cd "$_wd"'
                            ).format(d=a.dir)
                        )
                    else:
                        shell_steps.append(a)

                build_steps.append({
                    'name': action.name,
                    'shell': raw_shell(shell_join('\n', shell_steps)),
                    'required': True,
                })
            else:
                raise LegocastleStepError(
                    'You have a top-level action {0} that is not a step()'
                    .format(repr(action))
                )

        return shipit_projects, build_steps
