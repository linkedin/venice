#!/bin/sh

echo $1
echo $2
remote=$(git config --get remote.origin.url)
repo=$(basename $remote .git)
# POST a new ref to repo via Github API
curl -s -X POST https://api.github.com/repos/linkedin/venice/$repo/git/refs \
    -H "Authorization: token $2" \
    -d @- << EOF
    {
       "ref": "refs/tags/$1",
       "sha": "$commit"
    }
    EOF


