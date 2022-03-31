#!/export/apps/python/3.5/bin/python3.5

# To use, just execute the script from the root directory of your checked out venice repository
# It will parse the output of `git tag` to identify the correct next version, then it will
# /go/inclusivecode deferred(branch migrations will be done later)
# create the tag and push it to master

import sys
cur_version = sys.version_info
if (cur_version.major < 3 or (cur_version.major == 3 and cur_version.minor < 5)):
  raise Exception("This script requires at least python 3.5, using " + str(cur_version.major) + "." + str(cur_version.minor))
from subprocess import check_output
from subprocess import call

def getNextVersion():
  # looking to parse tags of form: "'release-0.1.36-cutoff'" to get latest version number and increment build compontent
  # asume tags end "-0.1.37-cutoff'" because we have some inconsistency in the prefix (sometimes release-, sometimes release-venice-)
  tags_text = check_output(["git", "tag"]) # tag1\ntag2\ntag3...
  tags_lines = [str(l) for l in tags_text.splitlines()] # ['tag1', 'tag2', ...]
  tags_parts = [l.split("-") for l in tags_lines] #[ ["'release", "0.1.36", "cutoff'"], ...]
  tags_numbers = [p[len(p)-2] for p in tags_parts if p[len(p)-1]=="cutoff'"] # ["0.1.36", ..]
  version_numbers = [n.split(".") for n in tags_numbers if len(n.split(".")) == 3] # [ ["0","1","36"], ...]
  version_ints = [[int(part) for part in number] for number in version_numbers] # [[0,1,36], ...]

  max_major = 0
  for v in version_ints: # [0, 1, 36]
    if (v[0] > max_major):
      max_major = v[0]

  major_filtered_version_numbers = [ n for n in version_ints if n[0]==max_major ]

  max_minor = 0
  for v in major_filtered_version_numbers: # [0, 1, 36]
    if (v[1] > max_minor):
      max_minor = v[1]

  minor_filtered_version_numbers = [ n for n in major_filtered_version_numbers if n[1]==max_minor ]

  max_build = 0
  for v in minor_filtered_version_numbers: # [0, 1, 36]
    if (v[2] > max_build):
      max_build = v[2]

  new_num = str(max_major) + "." + str(max_minor) + "." + str(max_build+1)
  return new_num

def getRemote():
  remote_text = check_output(["git", "remote", "-v"])
  lines = [l.decode('UTF-8') for l in remote_text.splitlines()]
  for line in lines:
    if "/venice/venice" in line:
      remote = str(line).split("\t")[0] # origin ssh://git.corp... (fetch)
      print("Using remote: " + remote)
      return remote
  raise Exception("Failed to parse remotes for this git repository")

def makeTag(remote):
  # /go/inclusivecode deferred(branch migrations will be done later)
  pull_success = call(["git", "pull", "--rebase", remote, "master"])
  if pull_success != 0:
    sys.exit()
  version = getNextVersion()
  tag_name = "release-" + version + "-cutoff"
  tag_message = "tag for release " + version
  tag_success = call(["git", "tag", "-a", "-m", tag_message, tag_name])
  if tag_success != 0:
    sys.exit()
  call(["git", "push", remote, tag_name])

def getTags(remote):
  # /go/inclusivecode deferred(branch migrations will be done later)
  call(["git", "fetch", remote, "master", "--tags"])

remote = getRemote()
getTags(remote)
makeTag(remote)



