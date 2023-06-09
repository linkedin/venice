---
layout: default
title: Venice Workspace Setup
parent: Developer Guides
permalink: /docs/dev_guide/workspace_setup
---

# Venice Workspace Setup

## Checking out the code
Venice's source code is hosted on [GitHub](https://www.github.com/linkedin/venice). Check out the code using:
```shell
git clone https://github.com/linkedin/venice.git
```

## Setting up the IDE
We recommend using IntelliJ IDEA for development, and provide instructions for it. However, any IDE of your choice
should work. To set up IntelliJ, run:
```shell
./gradlew idea
```

## Setting up your system
### Set the maximum number of open files limit
There are many resources on the web for updating the limits of maximum number of open files for each operating system.
We recommend setting a soft limit of at least `64000` and a hard limit of at least `524288`. Feel free to experiment
with various values and find ones that work for you.

## Build the project
```shell
./gradlew clean assemble
```

## Run the test suite
```shell
./gradlew check --continue
```

## Gradle Cheatsheet
```shell
# Build project classes and artifacts
./gradlew assemble
 
# Run all checks (spotbugs, code coverage, tests etc)
./gradlew check --continue
 
# Run only enabled checks that may fail the build
./gradlew spotbugs
 
# Run all checks, including ones that are in review/incubating stage, and do not fail the build
./gradlew spotbugs -Pspotallbugs -Pspotbugs.ignoreFailures
 
# Run jacoco code coverage check, which will also generate jacoco report
./gradlew jacocoTestCoverageVerification

# Run jacoco code coverage check along with jacoco report and diff coverage check
./gradlew jacocoTestCoverageVerification diffCoverage --continue
 
# Run enabled checks only for main code and in da-vinci-client subproject
./gradlew :clients:da-vinci-client:spotbugsMain

 # Run jacoco code coverage check for a specific module along with jacoco report
./gradlew :clients:da-vinci-client:jacocoTestCoverageVerification

# Run jacoco code coverage check for a specific module along with jacoco report and diff coverage check
./gradlew :clients:da-vinci-client:jacocoTestCoverageVerification diffCoverage

# Run a specific test in any module
$ ./gradlew :sub-module:testType --tests "fully.qualified.name.of.the.test"

# To run a specific integration test
$ ./gradlew :internal:venice-test-common:integrationTest --tests "fully.qualified.name.of.the.test"
```