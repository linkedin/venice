# Real-time topic deletion retention marker

`deprecated.real.time.topic.retention.ms` is a controller configuration with a default of **82,800,000 ms (23 hours)**.
It must be positive. Default truncation uses this value for regular and versioned RT topics, separate incremental-push
RT topics, and user system store RT topics, in both child and parent fabrics.

This value is a **reserved deletion marker**, not a delay. RT cleanup recognizes either this exact value or the existing
legacy deprecated-topic threshold (`deprecated.topic.max.retention.ms`, default 60 seconds), including old 15-second
markers. Negative, unlimited and unknown RT retentions are not deletion markers. Values immediately above or below the
RT marker are not eligible unless they meet the legacy threshold. Active RT retention must never equal the reserved
marker. The shared topic cleanup service scans each configured parent fabric or the child's default cluster. Existing
controller permission checks and applicable cleanup-cycle delays still apply; cleanup does not wait 23 hours.

The default truncation marker for VT, stream-reprocessing and view topics remains 15 seconds, with the existing
60-second cleanup threshold. These are deletion settings, not retention defaults for live topics. Fatal-DIV
version-topic debugging retention and its existing age-based cleanup remain unchanged.

## Rollout and backend requirements

All controllers that write or recognize markers must use the same value. Old controllers do **not** recognize the new
23-hour marker. To stage a mixed-version upgrade, keep the new setting at 15000 until all relevant controllers are
upgraded, then coordinate the change. Changing the setting again does not automatically recognize the previous custom
marker; drain or explicitly re-mark outstanding topics before switching values.

The pubsub backend must accept the retention update for truncation to mark a topic. This change does not bypass backend
policies or guarantee recovery from rejected or temporarily unavailable retention updates.
