---
layout: default
title: Venice Pull Request(PR) Guideline
parent: Developer Guides
---

# Venice Pull Request(PR) Guideline

## Creating GitHub issue

Every PR should be preceded by a GitHub issue to explain the problem statement unless it's a trivial bug fixes or a
documentation change. If your change is significant, please make sure your PR reviewers can align on the problem
statement via GitHub issues first.

The GitHub issue should contain the detailed problem statement.

## PR Description

Describe

* What changes to make and why you are making these changes.
* How are you going to achieve your goal
* Describe what testings you have done, for example, performance testing etc.

Checklist that might be helpful to facilitate the review:

* Design one-pager, design doc, or RFC
* GitHub Issue

### Added new dependencies?

Please list the new dependencies in the PR description and answer these questions for each new dependency

* What's their license?
* Are they compatible with our license?

## Attribution & Acknowledgements

This guide is based on the [Feathr Pull Request Guideline](https://linkedin.github.io/feathr/dev_guide/pull_request_guideline.html).