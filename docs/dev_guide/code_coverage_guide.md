---
layout: default
title: Code Coverage Guide
parent: Developer Guides
permalink: /docs/dev_guide/code_coverage_guide
---
# Code Coverage Guide

The Venice repository has two code coverage check on GitHub Action, submodule-level coverage verification
and new-commit coverage verification, which aim to improve the testability and code quality overall. The
GitHub Action fails the Pull Request if the coverage check doesn't meet the requirements. This guide provides the details
on how these reports are generated and how to debug them.

## Module structure

Venice has 4 modules, `all-modules`, `clients`, `internal` and `services`. Each module has its own submodules, except `all-modules`.

#### Clients

`da-vinci-client`, `venice-admin-tool`, `venice-client`, `venice-push-job`, `venice-samza`, `venice-thin-client`

#### Internal
`alpini`, `venice-avro-compatibility-test`, `venice-client-common`, `venice-common`, `venice-consumer`, 
`venice-jdk-compatibility-test`, `venice-test-common`

#### Services

`venice-router`, `venice-controller`, `venice-server`

## Submodule-level coverage verification

### Description
This will check the overall code coverage at the submodule level.
Jacoco generates the report based off the current submodule, performs the coverage verification and fails the build if the
coverage is below the threshold. The threshold, targets at the branch coverage, is defined in the `build.gradle` of each 
submodule independently.

### Example commands
```Bash
# Template
./gradlew :<module name>:<submodule name>:jacocoTestCoverageVerification 

./gradlew :clients:venice-push-job:jacocoTestCoverageVerification 
```

The report will be located at `<module name>/<submodule name>/build/reports/jacoco/test/index.html`.


### Debugging notes

#### Run commands against module doesn't generate the report

Due to the current project setup, running the commands, e.g. `./gradlew :clients:jacocoTestCoverageVerification`, doesn't
execute the unit tests thus no jacoco report will be generated. Please be sure to run the commands against the submodule.

## New-commit coverage verification

### Description
This will check the overall code coverage at the commit-level.
DiffCoverage, which is an extension of Jacoco, gathers the diff files by comparing the local branch and remote upstream 
main branch, and leverages the Jacoco report, to re-generate a new report only for these newly added lines/files. Similarly,
it performs the coverage verification and fails the build if the coverage is below the threshold. The threshold is defined
at 60% for branch coverage.

### Example commands
```Bash
# Template
./gradlew :<module name>:<submodule name>:jacocoTestCoverageVerification diffCoverage --continue

./gradlew :clients:venice-push-job:jacocoTestCoverageVerification diffCoverage --continue
```

The report will be located at `<module name>/build/reports/jacoco/diffCoverage/html/index.html`.

### Debugging notes

#### Integration tests are added but DiffCoverage doesn't identify the code coverage of it
Though integration tests are strongly encouraged, both Jacoco and DiffCoverage only work with unit tests so please consider writing
unit tests.

#### Unit tests are added but the DiffCoverage report doesn't reflect my changes

There are two possible reasons.
1. **Jacoco report isn't up-to-date**. DiffCoverage report relies on the Jacoco Report to reflect the correct coverage. 
If the Jacoco report isn't up-to-date, for example, executing the wrong command `./gradlew :clients:venice-push-job:diffCoverage` which
misses the step of re-running unit tests and updating the Jacoco report, can cause this issue. 
   1. Please be sure to run test and Jacoco report first.
2. **Units tests are placed in a different module**. Jacoco can only analyze and generate reports based off the current 
submodule. So, if you write new source codes in `submodule A` and `submodule B`, and you only have written unit tests in
`submodule B` which cover changes in `submodule A`, this cannot be recognized by Jacoco thus the DiffCoverage doesn't think
there's coverage too.
   1. Please move/re-organize some unit tests to the right submodule such that the coverage can be detected and reported.

#### The DiffCoverage report shows some files don't belong to my local changes

That's usually due to your local branch and upstream is not up-to-date so when it runs `git diff`, newly merged codes in
`linkedin/venice` are mistakenly treated as your changes.

Please do the followings:
1. Go to your Github fork(https://github.com/<user>/venice) and sync the `main` branch with upstream `linkedin:main`
2. Run `git fetch upstream` locally and pull the latest changes to your `main` branch
3. Merge `main` branch to your feature branch.
4. Confirm the diff are only your changes by running `git diff upstream/main`.

