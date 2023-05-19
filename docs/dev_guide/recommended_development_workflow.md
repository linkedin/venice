---
layout: default
title: Venice Recommended Development Workflow
parent: Developer Guides
permalink: /docs/dev_guide/recommended_development_workflow
---

# Venice Recommended Development Workflow

## Create a Design Document

If your change is relatively minor, you can skip this step. If you are adding new major functionality, we suggest that 
you add a design document and solicit comments from the community before submitting any code.

Please follow the [Design Document Guide](../dev_guide/design_doc.md). 


## Creating GitHub issue

Every PR should be preceded by a GitHub issue to explain the problem statement unless it's a trivial bug fixes or a
documentation change. If your change is significant, please make sure your PR reviewers can align on the problem
statement via GitHub issues first.

The GitHub issue should contain the detailed problem statement.

## Pull Request
1. Fork the GitHub repository at http://github.com/linkedin/venice if you haven't already
2. Clone your fork, create a new branch, push commits to the branch
3. Consider whether documentation or tests need to be added or updated as part of the change, and add them as needed (doc changes should be submitted along with code change in the same PR)
4. Run all tests as described in the project's [Workspace setup guide](../dev_guide/workspace_setup.md#run-the-test-suite).
5. Open a pull request against the `main` branch of `linkedin/venice`. (Only in special cases would the PR be opened against other branches.)
6. The PR title should usually be of the form `[component1]...[componentN]: Concise commit message`.
   * Valid tags are: `[da-vinci]` (or `[dvc]`), `[server]`, `[controller]`, `[router]`, `[samza]`,
      `[vpj]`, `[fast-client]` (or `[fc]`), `[thin-client]` (or `[tc]`), `[changelog]` (or `[cc]`),
      `[producer]`, `[admin-tool]`, `[test]`, `[build]`, `[doc]`, `[script]`, `[compat]`
   * `[compat]` tag means there are compatibility related changes in this PR, including upgrading protocol version, upgrading system store value schemas, etc. When there is a compatibility related change, it usually requires a specific deployment order, like upgrading controller before upgrading server. In this case, please explicitly call out the required deployment order in the commit message.
7. If the pull request is still a work in progress, and so is not ready to be merged, but needs to be pushed to GitHub to facilitate review,
    then create the PR as a [draft](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/about-pull-requests#draft-pull-requests) PR
   * If the PR cannot be created as a [draft](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/about-pull-requests#draft-pull-requests) PR,
     add `[WIP]` before the list of components.
8. Please state that the contribution is your original work and that you license the work to the project under the project's open source license.
9. If this PR resolves an issue be sure to include `Resolves #XXX` to correctly link and close the issue upon merge.
10. The project uses Apache Jenkins for continuous testing on Linux AMD64 and ARM64 build nodes. A CI job will not be started automatically for pull request. A maintainer has to trigger the testing. Feel free to tag a maintainer and ask for a build trigger.
11. Once ready, a maintainer will update the PR with the test results.
12. Investigate and fix failures caused by the pull the request
13. Fixes can simply be pushed to the same branch from which you opened your pull request.
14. Please address feedback via additional commits instead of amending existing commits. This makes it easier for the reviewers to know what has changed since the last review. All commits will be squashed into a single one by the committer via GitHub's squash button or by a script as part of the merge process.
15. Jenkins will automatically re-test when new commits are pushed.
16. Despite our efforts, Venice may have flaky tests at any given point, which may cause a build to fail. You need to ping committers to trigger a new build. If the failure is unrelated to your pull request and you have been able to run the tests locally successfully, please mention it in the pull request.

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

## The Review Process
1. Other reviewers, including committers, may comment on the changes and suggest modifications. Changes can be added by simply pushing more commits to the same branch.
2. Please add a comment and "@" the reviewer in the PR if you have addressed reviewers' comments. Even though GitHub sends notifications when new commits are pushed, it is helpful to know that the PR is ready for review once again.
3. Lively, polite, rapid technical debate is encouraged from everyone in the community. The outcome may be a rejection of the entire change.
4. Reviewers can indicate that a change looks suitable for merging by approving it via GitHub's review interface. This indicates the strongest level of technical sign-off on a patch and it means: "I've looked at this thoroughly and take as much ownership as if I wrote the patch myself". If you approve a pull request, you will be expected to help with bugs or follow-up issues on the patch. Consistent, judicious use of pull request approvals is a great way to gain credibility as a reviewer with the broader community. Venice reviewers will typically include the acronym LGTM in their approval comment. This was the convention used to approve pull requests before the "approve" functionality was introduced by GitHub.
5. Sometimes, other changes will be merged which conflict with your pull request's changes. The PR can't be merged until the conflict is resolved. This can be resolved with `"git fetch upstream"` followed by `"git rebase upstream/main"` and resolving the conflicts by hand, then pushing the result to your branch.
6. Try to be responsive to the discussion rather than let days pass between replies.

## Closing Your Pull Request / Issue
1. If a change is accepted, it will be merged and the pull request will automatically be closed, along with the associated Issue if the PR description contains `Resolves #XXX`
2. Note that in the rare case you are asked to open a pull request against a branch besides `main`, that you will actually have to close the pull request manually
3. If your pull request is ultimately rejected, please close it.
4. If a pull request has gotten little or no attention, consider improving the description or the change itself and ping likely reviewers after a few days. Consider proposing a change that's easier to include, like a smaller and/or less invasive change.
5. If a pull request is closed because it is deemed not the right approach to resolve an Issue, then leave the Issue open. However, if the review makes it clear that the problem identified in the Issue is not going to be resolved by any pull request (not a problem, won't fix) then also resolve the Issue.

## Attribution & Acknowledgements

This guide is based on the [Contributing Code Changes](https://cwiki.apache.org/confluence/display/KAFKA/Contributing+Code+Changes) guide from the [Apache Kafka](https://kafka.apache.org/) project.