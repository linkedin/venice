<!--
Add a list of affected components in the PR title in the following format:
[component1]...[componentN] Concise commit message

Valid component tags are: [da-vinci] (or [dvc]), [server], [controller], [router], [samza],
[vpj], [fast-client] (or [fc]), [thin-client] (or [tc]), [changelog] (or [cc]),
[pulsar-sink], [producer], [admin-tool], [test], [build], [doc], [script], [compat]

Example title: [server][da-vinci] Use dedicated thread to persist data to storage engine

Note: PRs with titles not following the format will not be merged
-->

## Problem Statement
<!--
Describe
- What problem are you trying to solve? - What issues or limitations exist in the current code? 
-->
## Solution
<!--
Describe
- What changes you are making and why. - How these changes solve the problem.
- Any performance considerations or trade-offs. 
- Describe what testings you have done, for example, performance testing etc.
-->


###  Code changes
- [ ] Added new code behind **a config**. If so list the config names and their default values.
- [ ] If introducing new **log lines** and if the log needs to be **rate limited**.

### 🔥 **Concurrency-Specific Checks**
- [ ] Code has **no race conditions** or **thread safety issues**.
- [ ] Proper **synchronization mechanisms** (e.g., `synchronized`, `RWLock`) are used where needed.
- [ ] No **blocking calls** inside critical sections that could lead to deadlocks.
- [ ] Verified **thread-safe collections** are used (e.g., `ConcurrentHashMap`, `CopyOnWriteArrayList`).

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
- [ ] Yes. Make sure to explain your proposed changes and call out the behavior change.