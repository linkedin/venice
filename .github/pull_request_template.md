<!--
Add a list of affected components in the PR title in the following format:
[component1]...[componentN] Concise commit message

Valid component tags are: [da-vinci] (or [dvc]), [server], [controller], [router], [samza],
[vpj], [fast-client] (or [fc]), [thin-client] (or [tc]), [changelog] (or [cc]),
[pulsar-sink], [producer], [admin-tool], [test], [build], [doc], [script], [compat], [protocol]

Example title: [server][da-vinci] Use dedicated thread to persist data to storage engine

Note: PRs with titles not following the format will not be merged
-->

## Problem Statement
<!--
Describe
- What problem are you trying to solve
- What issues or limitations exist in the current code
- Why this change is necessary 
-->


## Solution
<!--
Describe
- What changes you are making and why. 
- How these changes solve the problem.
- Any performance considerations or trade-offs. 
- Describe what testings you have done, for example, performance testing etc.
-->


###  Code changes
- [ ] Added new code behind **a config**. If so list the config names and their default values in the PR description.
- [ ] Introduced new **log lines**. 
  - [ ] Confirmed if logs need to be **rate limited** to avoid excessive logging.

###  **Concurrency-Specific Checks**
Both reviewer and PR author to verify
- [ ] Code has **no race conditions** or **thread safety issues**.
- [ ] Proper **synchronization mechanisms** (e.g., `synchronized`, `RWLock`) are used where needed.
- [ ] No **blocking calls** inside critical sections that could lead to deadlocks or performance degradation.
- [ ] Verified **thread-safe collections** are used (e.g., `ConcurrentHashMap`, `CopyOnWriteArrayList`).
- [ ] Validated proper exception handling in multi-threaded code to avoid silent thread termination.


## How was this PR tested?
<!--
If you're unsure about what to test, where to add tests, or how to run tests, please feel free to ask. We'd be happy to help.
-->

- [ ] New unit tests added.
- [ ] New integration tests added.
- [ ] Modified or extended existing tests.
- [ ] Verified backward compatibility (if applicable).

## Does this PR introduce any user-facing or breaking changes?
<!--  
If yes, please clarify the previous behavior and the change this PR proposes - provide the console output, description and/or an example to show the behavior difference if possible.
If no, choose 'No'.
-->
- [ ] No. You can skip the rest of this section.
- [ ] Yes. Clearly explain the behavior change and its impact.