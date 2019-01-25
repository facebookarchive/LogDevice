#!/usr/bin/env python
# Copyright (c) Facebook, Inc. and its affiliates.
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
'''

To run open-source builds on Legocastle, we:
 1) Process our dependencies with ShipIt
 2) Commit the results into our repo: `commit_to_hg`,
 3) Save all non-trunk changes in the repo (ignoring untracked files) to
    an Everstore bundle: `get_local_repo_everstore_bundle_lego_dict`.

'''
import logging
import os
import subprocess
import tempfile

from contextlib import contextmanager
from utils import run_command


class HgRepo(object):
    def __init__(self, path):
        self.env = os.environ.copy()
        self.env['HGPLAIN'] = '1'

        self.path = subprocess.check_output(
            ['hg', 'root'], cwd=path, env=self.env
        ).strip()

    def run_hg(self, *args):
        run_command('hg', *args, cwd=self.path, env=self.env)

    def get_hg_output(self, *args):
        cmd = ['hg']
        cmd.extend(args)
        return subprocess.check_output(cmd, cwd=self.path, env=self.env)


@contextmanager
def commit_to_hg(repo, d, message):
    orig_hash = identify_cur_repo_hash(repo)
    repo.run_hg('add', d)
    try:
        # Some of our open source repositories unfortunately have tracked
        # files that match their ignore patterns.  Explicitly add those
        # files too.
        ignored_output = repo.get_hg_output('status', '-in0', d)
        ignored_files = ignored_output.split('\0')
        if ignored_files and ignored_files[-1] == '':
            ignored_files = ignored_files[:-1]
        if ignored_files:
            repo.run_hg('add', '--', *ignored_files)

        repo.run_hg('commit', '-m', message, d)
    except BaseException:  # clean up even on Ctrl-C
        repo.run_hg('forget', d)
        raise
    try:
        new_hash = identify_cur_repo_hash(repo)
        logging.info(
            'Temporarily committed ShipIt-processed projects to '
            'HG as commit %s', new_hash
        )
        yield new_hash
    finally:
        repo.run_hg('update', orig_hash)
        repo.run_hg('hide', new_hash)
    logging.info('Stripped ShipIt-processed projects from HG')


def identify_cur_repo_hash(repo):
    return repo.get_hg_output('log', '-T{node}', '-r.').strip()


def get_local_repo_everstore_bundle_lego_dict(repo):
    cur_hash = identify_cur_repo_hash(repo)
    ancestor = repo.get_hg_output(
        'log', '-T{node}', '-r', 'ancestor(master, {})'.format(cur_hash)
    ).strip()

    with tempfile.NamedTemporaryFile() as bundle:
        try:
            repo.run_hg(
                'bundle',
                '--base', ancestor,
                '-r', '{}%{}'.format(cur_hash, ancestor),
                bundle.name,
            )
        except subprocess.CalledProcessError as ex:
            if ex.returncode == 1:  # No changes, as per `hg bundle --help`.
                return {'hash': cur_hash}
            raise

        handle = subprocess.check_output([
            'clowder', 'put', '--fbtype', '3544', bundle.name]).strip()

        return {
            'hash': cur_hash,
            'pullHost': '@everstore:{0}'.format(handle),
        }
