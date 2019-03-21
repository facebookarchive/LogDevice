#!/usr/bin/env python
# Copyright (c) Facebook, Inc. and its affiliates.
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
'''
WARNING: Mutates your current repo. Read --help before running.

Reads `facebook_fbcode_builder_config.py` from the current directory,
temporarily commits all relevant ShipIt-processed projects into your current
repo, prints to stdout a JSON Legocastle job to build your project and its
dependencies.

Try `.../facebook_make_legocastle_job.py --help` from a project's `build/`
directory -- or read BASIC_HELP below.
'''

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys

from facebook_fbsource_utils import (
    HgRepo, commit_to_hg, get_local_repo_everstore_bundle_lego_dict
)
from facebook_legocastle_builder import LegocastleFBCodeBuilder
from shell_quoting import ShellQuoted
from utils import make_temp_dir, run_command
from utils import read_fbcode_builder_config, build_fbcode_builder_config

# Used in the command-line --help.
BASIC_HELP = '''

WARNING: This transiently mutates your repo, and then cleans up. Do NOT run
concurrently with `hg` write operations, or you will have a bad time.

Basic operation:
 - reads `facebook_fbcode_builder_config.py` from the specified
   project directories,
 - processes your current repo with ShipIt,
 - (temporarily) commits the results under
   fbcode/opensource/fbcode_builder/shipit_projects,
 - publishes the resulting bundle to EverStore,
 - creates Legocastle jobs that builds the projects using that bundle.

NOTE: Untracked files are ignored.

Jobs can be started via a determinator launched by SandcastleStep in www,
or directly on the command-line from fbcode, e.g.

  {0} PROJECT1 PROJECT2 | scutil create

== Legocastle debugging hints ==

0) If possible, reproduce inside a VM you own, using Docker. See e.g.
   `travis_docker_build.sh` and `fbcode_builder/README.docker`. Check
   Legocastle's Diagnostics step to figure out the OS image & GCC version.

1) If Legocastle is required, insert `|| sleep 10800` after the failing
   command to prevent Legocastle from reaping your job's box instantly.
   Future: it might make sense to add a debug mode which automatically adds
   this to most / all commands.

2) Use sush + secrets_tool get FOG_ROOT_PASSWORD to log into the Sandcastle
   VM (see the `hostname` diagnostic).

'''.format(os.path.basename(sys.argv[0]))


def parse_args():
    desc = ('Print legocastle job definitions to perform OSS builds of the '
            'specified projects.  Also runs shipit to generate the code '
            'to build.\n') + BASIC_HELP
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument(
        '--use-everstore-bundle', nargs=2, metavar=('HASH', 'PULL_HOST'),
        help='''

        (For development / debugging) Instead of packaging code from the
        current HG repo, use these pre-existing values (see Legocastle
        keys "hash" and "pullHost").  This saves a lot of time waiting
        for new ShipIt and HG bundles when you are only changing the
        build steps.

        ''',
    )
    parser.add_argument(
        '--prefix', metavar='DIR', default=None,
        help='Install all libraries in this prefix.',
    )
    parser.add_argument(
        '--make-parallelism', metavar='NUM', type=int,
        help='Use `make -j` on multi-CPU systems with lots of RAM.',
    )
    parser.add_argument(
        '--projects-dir', metavar='DIR',
        help='Place project code directories here.',
    )
    parser.add_argument(
        '--shipit-project-dir',
        default='fbcode/opensource/fbcode_builder/shipit_projects',
        help='The location to commit shipit-generated project directories.'
    )
    parser.add_argument('--debug', action='store_true', help='Log more')
    parser.add_argument('projects', nargs='*')
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(levelname)s: %(message)s'
    )

    return args


def make_lego_spec(builder, type):
    capabilities = {
        'type': type,
        'vcs': 'fbcode-fbsource',
        'os': builder.option('legocastle_os', 'stable'),
    }
    xcode = builder.option('xcode_version', '')
    if xcode != '':
        capabilities['xcode'] = xcode
    tenant = builder.option('tenant', '')
    if tenant != '':
        capabilities['tenant'] = tenant
    return {
        'alias': builder.option('alias'),
        'command': 'SandcastleUniversalCommand',
        'capabilities': capabilities,
        'priority': 3, # See SandcastleFBCodeUtils::getSuggestedChildPriority
        'args': {
            'name': builder.option('build_name'),
            'oncall': builder.option('oncall'),
            'timeout': 14400,
            # We will need to download gtest/gmock.
            'env': {'https_proxy': 'fwdproxy:8080'},
            'steps': [],
        },
    }


LEGO_OPTS_MAP = {
    'legocastle_opts': "lego-linux",
    'legocastle_opts_macos': "lego-mac",
}


def make_lego_jobs(args, project_dirs):
    '''
    Compute the lego job specifications.

    Returns a tuple of (shipit_projects, job_specs)
    '''
    install_dir = ShellQuoted(  # BOX_DIR is our fbcode path.
        '"$BOX_DIR/opensource/fbcode_builder/facebook_ci"'
    )

    # The following project-specific options may be overridden from the
    # command-line options.  If they are not specified on the command line or
    # in the project configuration file, then the following defaults are used.
    project_option_defaults = {
        'prefix': install_dir,
        'make_parallelism': 8,
        'projects_dir': install_dir,
    }

    # Process all of the project configs
    all_shipit_projects = set()
    children_jobs = []
    for project in project_dirs:
        logging.debug('Processing %s', project)
        config_path = os.path.join(project, 'facebook_fbcode_builder_config.py')
        config = read_fbcode_builder_config(config_path)

        for opts_key, lego_type in LEGO_OPTS_MAP.items():
            config_opts = config.get(opts_key)
            if not config_opts:
                continue

            # Construct the options for this project.
            # Use everything listed in config_opts, unless the value is None.
            project_opts = {
                opt_name: value
                for opt_name, value in config_opts.items() if value is not None
            }

            # Allow options specified on the command line to override the
            # config's legocastle_opts data.
            # For options that weren't provided in either place, use the default
            # value from project_option_defaults.
            for opt_name, default_value in project_option_defaults.items():
                cli_value = getattr(args, opt_name)
                if cli_value is not None:
                    project_opts[opt_name] = cli_value
                elif opt_name not in config_opts:
                    project_opts[opt_name] = default_value

            # The shipit_project_dir option cannot be overridden on a per-project
            # basis.  We emit this data in a single location that must be consisten
            # across all of the projects we are building.
            project_opts['shipit_project_dir'] = args.shipit_project_dir

            builder = LegocastleFBCodeBuilder(**project_opts)
            steps = build_fbcode_builder_config(config)(builder)

            lego_spec = make_lego_spec(builder, type=lego_type)

            shipit_projects, lego_spec['args']['steps'] = builder.render(steps)
            all_shipit_projects.update(shipit_projects)
            children_jobs.append(lego_spec)

    return all_shipit_projects, children_jobs


def run_shipit(fbsource, project_name, output_dir):
    logging.debug('Running shipit for %s', project_name)

    cmd = [
        'php',
        '/var/www/scripts/opensource/shipit/run_shipit.php',
        '--project=' + project_name,
        '--create-new-repo',
        '--source-repo-dir=' + fbsource,
        '--source-branch=.',
        '--skip-source-init',
        '--skip-source-pull',
        '--skip-source-clean',
        '--skip-push',
        '--skip-reset',
        '--skip-project-specific',
        '--destination-use-anonymous-https',
        '--create-new-repo-output-path=' + output_dir,
    ]
    run_command(*cmd)

    # Remove the .git directory from the repository it generated.
    # There is no need to commit this.
    repo_git_dir = os.path.join(output_dir, '.git')
    shutil.rmtree(repo_git_dir)

    # Proxygen requires a VERSION file that only exists in the github
    # repository.
    # TODO: this file is stale and not updated any more.  We should just update
    # the proxygen build to not require it.
    if project_name == 'proxygen':
        with open(os.path.join(output_dir, 'proxygen', 'VERSION'), 'w') as f:
            f.write('32:0\n')


def create_shipit_commit(fbsource, shipit_projects, output_dir):
    abs_output_dir = os.path.join(fbsource.path, output_dir)
    with make_temp_dir(abs_output_dir):
        for project in shipit_projects:
            project_output_dir = os.path.join(abs_output_dir, project)
            run_shipit(fbsource.path, project, project_output_dir)

        with commit_to_hg(fbsource, output_dir, 'ShipIt-processed projects'):
            bundle_info = get_local_repo_everstore_bundle_lego_dict(fbsource)

    return bundle_info


def main():
    args = parse_args()

    # Find the fbsource repository root
    fbsource = HgRepo(os.path.dirname(sys.argv[0]))

    # If no projects were explicitly listed on the command line,
    # assume the current working directory contains the only directory to
    # process.  (This is the older mode of invoking this script back when it
    # only supported processing one project at a time.)
    project_dirs = args.projects
    old_style_output = False
    if not project_dirs:
        # Find the project configuration from the current working directory
        project_dirs = ['.']
        old_style_output = True

    shipit_projects, lego_jobs = make_lego_jobs(args, project_dirs)
    logging.info('Projects using ShipIt: {0}'.format(shipit_projects))

    # Compute the bundle containing the shipit-processed projects.
    # If --use-everstore-bundle was supplied, use that data.
    # Otherwise, run shipit ourselves to get the data.
    if args.use_everstore_bundle:
        bundle_info = {
            'hash': args.use_everstore_bundle[0],
            'pullHost': args.use_everstore_bundle[1],
        }
    else:
        bundle_info = create_shipit_commit(
            fbsource, shipit_projects, args.shipit_project_dir
        )

    # Update the job specs with the bundle information
    for job in lego_jobs:
        job.update(bundle_info)

    if old_style_output:
        # The old invocation syntax expects a single job to be emitted,
        # rather than an array
        lego_jobs = lego_jobs[0]

    print(json.dumps(lego_jobs, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
