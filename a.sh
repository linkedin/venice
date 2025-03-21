#!/usr/bin/env bash


  prev_tag=$(git for-each-ref --sort=creatordate --format '%(refname:lstrip=2)' refs/tags |  tail -2 | head -1)
  curr_tag=$(git for-each-ref --sort=creatordate --format '%(refname:lstrip=2)' refs/tags | tail -1)
  echo "Previous tag: ${prev_tag}"
  echo "Current tag: ${curr_tag}"
  for commit in $(git rev-list "$prev_tag..$curr_tag"); do
      git --no-pager log --no-decorate "$commit^..$commit" --oneline >> changelog.txt
  done
