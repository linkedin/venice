#!/usr/bin/env python3

# To use, just execute the script from the root directory of your checked out venice repository
# It will parse the output of `git tag` to identify the correct next version, then it will
# /go/inclusivecode deferred(branch migrations will be done later)
# create the tag and push it to master

import sys
from subprocess import check_output
from subprocess import call
import click

cur_version = sys.version_info
if cur_version.major < 3 or (cur_version.major == 3 and cur_version.minor < 6):
    raise Exception(f"This script requires at least python 3.6, using {cur_version.major}.{cur_version.minor}")


@click.command()
@click.option("--bump-major", is_flag=True)
@click.option("--bump-minor", is_flag=True)
@click.option("--no-verify", is_flag=True)
def read_config(bump_major, bump_minor, no_verify):
    if bump_major and bump_minor:
        print("Cannot bump major and minor versions. Only bumping major version")
        bump_minor = False

    run(bump_major, bump_minor, not no_verify)


def format_version(major, minor, build):
    return f"{major}.{minor}.{build}"


def get_next_version(bump_major, bump_minor):
    # looking to parse tags of form: "'release-0.1.36-cutoff'" to get latest version number and increment build
    # component assume tags end "-0.1.37-cutoff'" because we have some inconsistency in the prefix (sometimes
    # release-, sometimes release-venice-)
    tags_text = check_output(["git", "tag"])  # tag1\ntag2\ntag3...
    tags_lines = [str(l) for l in tags_text.splitlines()]  # ['tag1', 'tag2', ...]
    tags_parts = [l.split("-") for l in tags_lines]  # [ ["'release", "0.1.36", "cutoff'"], ...]
    tags_numbers = [p[len(p) - 2] for p in tags_parts if p[len(p) - 1] == "cutoff'"]  # ["0.1.36", ..]
    version_numbers = [n.split(".") for n in tags_numbers if len(n.split(".")) == 3]  # [ ["0","1","36"], ...]
    version_ints = [[int(part) for part in number] for number in version_numbers]  # [[0,1,36], ...]

    max_major = 0
    for v in version_ints:  # [0, 1, 36]
        max_major = max(max_major, v[0])

    if bump_major:
        max_major += 1

    major_filtered_version_numbers = [n for n in version_ints if n[0] == max_major]

    if bump_major:
        if major_filtered_version_numbers:
            raise Exception(f"""
            Something went wrong. Expected version list for major version {max_major}
            to be empty. Found {major_filtered_version_numbers}. Verify the script.
            """)

        return format_version(max_major, 0, 0)

    max_minor = 0
    for v in major_filtered_version_numbers:  # [0, 1, 36]
        max_minor = max(max_minor, v[1])

    if bump_minor:
        max_minor += 1

    minor_filtered_version_numbers = [n for n in major_filtered_version_numbers if n[1] == max_minor]

    if bump_minor:
        if minor_filtered_version_numbers:
            raise Exception(f"""
            Something went wrong. Expected version list for major version {max_major} and minor version {max_minor}
            to be empty. Found {minor_filtered_version_numbers}. Verify the script.
            """)

        return format_version(max_major, max_minor, 0)

    max_build = 0
    for v in minor_filtered_version_numbers:  # [0, 1, 36]
        if v[2] > max_build:
            max_build = v[2]

    return format_version(max_major, max_minor, max_build + 1)


def get_remote():
    remote_text = check_output(["git", "remote", "-v"])
    lines = [l.decode("UTF-8") for l in remote_text.splitlines()]
    for line in lines:
        # TODO: Change to GitHub repo
        if "/venice/venice" in line:
            remote = str(line).split("\t")[0]  # origin ssh://git.corp... (fetch)
            print("Using remote: " + remote)
            return remote
    raise Exception("Failed to parse remotes for this git repository")


def make_tag(remote, bump_major, bump_minor, need_verification):
    # /go/inclusivecode deferred(branch migrations will be done later)
    pull_success = call(["git", "pull", "--rebase", remote, "master"])
    if pull_success != 0:
        sys.exit()
    version = get_next_version(bump_major, bump_minor)
    tag_name = "release-" + version + "-cutoff"
    tag_message = "tag for release " + version

    if need_verification:
        proceed = get_confirmation(f"New tag is {tag_name}. Continue? [y/N]: ")
        if not proceed:
            print("Skipped creating the tag")
            return

    tag_success = call(["git", "tag", "-a", "-m", tag_message, tag_name])
    if tag_success != 0:
        sys.exit()
    call(["git", "push", remote, tag_name])


def get_tags(remote):
    # /go/inclusivecode deferred(branch migrations will be done later)
    call(["git", "fetch", remote, "master", "--tags"])


def run(bump_major, bump_minor, need_verification):
    remote = get_remote()
    get_tags(remote)
    make_tag(remote, bump_major, bump_minor, need_verification)


def get_confirmation(prompt=None):
    yes = {"yes", "y", "ye"}
    if prompt:
        prompt = prompt + " "
        choice = input(prompt).lower()
    else:
        choice = input("Are you sure? ").lower()

    return choice in yes


if __name__ == "__main__":
    read_config()
