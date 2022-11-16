#!/usr/bin/env python3

# To use, just execute the script from the root directory of your checked out venice repository
# It will parse the output of `git tag` to identify the correct next version, then it will
# create the tag and push it to main

import sys
import requests
from subprocess import check_output
from subprocess import call
import click
import re

cur_version = sys.version_info
if cur_version.major < 3 or (cur_version.major == 3 and cur_version.minor < 6):
    raise Exception(f'This script requires at least python 3.6, using {cur_version.major}.{cur_version.minor}')


@click.command()
@click.option('--bump-major', is_flag=True)
@click.option('--bump-minor', is_flag=True)
@click.option('--no-verify', is_flag=True)
@click.option('--github-actor', help='The github actor used for push')
def read_config(bump_major, bump_minor, no_verify, github_actor):
    if bump_major and bump_minor:
        print('Cannot bump major and minor versions. Only bumping major version')
        bump_minor = False

    run(bump_major, bump_minor, not no_verify, github_actor)


def format_version(major, minor, build):
    return f'{major}.{minor}.{build}'


def get_next_version(bump_major, bump_minor, remote):
    tag_text = check_output(['git', 'describe', '--tags', '--abbrev=0', f'{remote}/main'], text=True)
    tag_text = tag_text.rstrip()
    commits = check_output(['git', 'log', f'{tag_text}..HEAD', '--oneline'], text=True)
    if commits == "":
        print(f'Not pushing new tag as there is no new commit since the last tag {tag_text}')
        sys.exit("Tag not pushed as no new commit")
    version_numbers = tag_text.split('.')  # ['0','1','36']
    version_ints = [int(part) for part in version_numbers]  # [0,1,36]

    major = version_ints[0]
    if bump_major:
        major += 1
        return format_version(major, 0, 0)

    minor = version_ints[1]
    if bump_minor:
        minor += 1
        return format_version(major, minor, 0)

    build = version_ints[2]

    return format_version(major, minor, build + 1)


def get_remote():
    remote_text = check_output(['git', 'remote', '-v'])
    lines = [l.decode('UTF-8') for l in remote_text.splitlines()]
    for line in lines:
        if 'git@github.com:linkedin/venice' in line or 'https://github.com/linkedin/venice' in line:
            remote = str(line).split('\t')[0]  # origin ssh://git@github.com... (fetch)
            print('Using remote: ' + remote)
            return remote
    raise Exception('Failed to parse remotes for this git repository')


def make_tag(remote, bump_major, bump_minor, need_verification, github_actor):
    if github_actor:
        set_user=call(['git', 'config', 'user.name', github_actor])
        if set_user != 0:
            sys.exit("Could not set user.name config")
        set_email=call(['git', 'config', 'user.email', f'{github_actor}@users.noreply.github.com'])
        if set_email != 0:
            sys.exit("Could not set user.email config")

    pull_success = call(['git', 'pull', '--rebase', remote, 'main'])
    if pull_success != 0:
        sys.exit("Could not rebase with remote")
    version = get_next_version(bump_major, bump_minor, remote)
    tag_message = 'tag for release ' + version

    if need_verification:
        proceed = get_confirmation(f'New tag is {version}. Continue? [y/N]: ')
        if not proceed:
            print('Skipped creating the tag')
            return

    tag_success = call(['git', 'tag', '-a', '-m', tag_message, version])
    if tag_success != 0:
        sys.exit("Could not tag")
    call(['git', 'push', remote, version])


def get_tags(remote):
    call(['git', 'fetch', remote, 'main', '--tags'])
    call(['git', 'fetch', remote, 'main'])


def run(bump_major, bump_minor, need_verification, github_actor):
    remote = get_remote()
    get_tags(remote)
    make_tag(remote, bump_major, bump_minor, need_verification, github_actor)


def get_confirmation(prompt=None):
    yes = {'yes', 'y', 'ye'}
    if prompt:
        prompt = prompt + ' '
        choice = input(prompt).lower()
    else:
        choice = input('Are you sure? ').lower()

    return choice in yes


if __name__ == '__main__':
    read_config()
