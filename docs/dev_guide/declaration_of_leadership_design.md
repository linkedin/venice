# Declaration of Leadership (DoL) Design for Fast Leader Handover

## Problem Statement

The current `canSwitchToLeaderTopic()` implementation relies on a time-based mechanism (waiting for `newLeaderInactiveTime`) to determine when it's safe to switch from consuming the local version topic (VT) to the leader source topic (remote VT during batch push, or RT topic for hybrid stores). This approach has reliability issues:

1. **Non-deterministic timing**: Potential Kafka consumer starvation could cause the old leader to continue producing even after the wait period
2. **Downstream detection burden**: Followers must detect upstream offset rewind via producer hostname changes
3. **Lack of deterministic confirmation**: No explicit confirmation that the new leader has established its position in the VT

## Proposed Solution: Declaration of Leadership (DoL)

Use a **loopback mechanism** where the new leader explicitly declares its leadership by:
1. Appending a special DoL message to the local VT
2. Waiting until it consumes this message back from the VT (loopback confirmation)
3. Only after confirmation, switching to consume from the leader source topic (remote VT or RT)

This provides a **deterministic guarantee** that the leader has successfully written to and consumed from the VT before transitioning to the leader source topic.

---

## Design Details

### 1. DoL Message Format

**Use `START_OF_SEGMENT` control message with special metadata and dedicated key:**

- **Message Type**: `ControlMessageType.START_OF_SEGMENT`
- **Key**: `KafkaKey.DECLARATION_OF_LEADERSHIP` (new static constant to add to `KafkaKey.java`)
  - Similar structure to `KafkaKey.HEART_BEAT` but with distinct GUID for semantic clarity
  - Distinguishes DoL messages from regular heartbeat messages
  - Enables clean filtering and monitoring of DoL messages
- **Special Metadata** (in `StartOfSegment` payload):
  - **`isDeclarationOfLeadership`**: boolean flag (new field to add to `StartOfSegment.avsc`)
  - **`leadershipTerm`**: long value representing the leadership term/epoch
  - **`producerGUID`**: GUID identifying the new leader replica
  - **`declarationTimestamp`**: timestamp when DoL was issued

**Why START_OF_SEGMENT with dedicated DoL key?**
- Semantic alignment: declaring leadership is starting a new segment of leader activity
- Minimal schema changes: add optional fields to existing `StartOfSegment` message
- Existing infrastructure already handles START_OF_SEGMENT control messages
- **Dedicated key prevents confusion with heartbeat messages** and enables clear distinction
- Clean separation of concerns: DoL != heartbeat

### 2. State Tracking in PartitionConsumptionState

Add new fields to track DoL lifecycle:

```java
/**
 * Tracks the Declaration of Leadership (DoL) state for fast leader handover.
 * Only relevant when transitioning from STANDBY to LEADER.
 */
public static class DeclarationOfLeadershipState {
  // Whether DoL message has been produced to local VT
  private volatile boolean dolMessageProduced = false;
  
  // The future tracking the DoL message produce operation
  private volatile CompletableFuture<PubSubProduceResult> dolProduceFuture = null;
  
  // The position where DoL message was produced (null until confirmed)
  private volatile PubSubPosition dolPosition = null;
  
  // The leadership term included in the DoL message
  private volatile long leadershipTerm = 0;
  
  // Timestamp when DoL was produced
  private volatile long dolProducedTimestamp = 0;
  
  // Whether we've consumed the DoL message back (loopback confirmation)
  private volatile boolean dolConsumed = false;
  
  // Timestamp when DoL was consumed back
  private volatile long dolConsumedTimestamp = 0;
  
  // Reset state when transitioning out of leader or back to standby
  public void reset() {
    this.dolMessageProduced = false;
    this.dolProduceFuture = null;
    this.dolPosition = null;
    this.leadershipTerm = 0;
    this.dolProducedTimestamp = 0;
    this.dolConsumed = false;
    this.dolConsumedTimestamp = 0;
  }
  
  // Getters and setters...
}

// In PartitionConsumptionState class:
private final DeclarationOfLeadershipState dolState = new DeclarationOfLeadershipState();

public DeclarationOfLeadershipState getDolState() {
  return dolState;
}
```

### 3. Updated `canSwitchToLeaderTopic()` Logic

Replace the time-based check with DoL-based confirmation:

```java
/**
 * Checks whether the replica can safely switch to consuming from the leader source topic.
 * 
 * <p>Uses Declaration of Leadership (DoL) loopback mechanism:
 * 1. DoL message must have been produced to local VT
 * 2. DoL message must have been consumed back from local VT (loopback confirmation)
 * 
 * <p>The DoL loopback confirmation provides a strong guarantee that:
 * - The new leader can successfully write to the local VT
 * - The new leader can successfully consume from the local VT
 * - The new leader has established its position in the VT before switching to leader source topic
 * - **The new leader has fully caught up on the VT** (consuming the DoL back means
 *   the leader has processed all messages up to and including its own DoL message)
 * 
 * <p>This eliminates the need for time-based waits or special handling for different
 * store types (e.g., user system stores). The loopback mechanism is sufficient for all stores
 * because consuming the DoL back is inherently a confirmation of being caught up on VT.
 * 
 * <p>The leader source topic can be either:
 * - Remote VT during batch push (before EOP)
 * - RT topic for hybrid stores (after EOP)
 * 
 * @param pcs partition consumption state
 * @return true if safe to switch to leader topic; false otherwise
 */
private boolean canSwitchToLeaderTopic(PartitionConsumptionState pcs) {
  DeclarationOfLeadershipState dolState = pcs.getDolState();
  
  // Step 1: Check if DoL message has been produced
  if (!dolState.isDolMessageProduced()) {
    LOGGER.debug(
        "Cannot switch to leader topic for partition {}: DoL message not yet produced",
        pcs.getPartition());
    return false;
  }
  
  // Step 2: Check if DoL message has been consumed back (loopback confirmation)
  if (!dolState.isDolConsumed()) {
    // Check if DoL produce operation is still pending
    CompletableFuture<PubSubProduceResult> produceFuture = dolState.getDolProduceFuture();
    if (produceFuture != null && !produceFuture.isDone()) {
      LOGGER.debug(
          "Cannot switch to leader topic for partition {}: DoL produce operation still pending",
          pcs.getPartition());
      return false;
    }
    
    // Produce completed but not consumed yet - this is expected during normal operation
    LOGGER.debug(
        "Cannot switch to leader topic for partition {}: DoL message produced at {} but not yet consumed back",
        pcs.getPartition(),
        dolState.getDolPosition());
    return false;
  }
  
  // All conditions met - safe to switch to leader topic
  // The DoL loopback confirmation is sufficient for all store types
  long loopbackLatency = dolState.getDolConsumedTimestamp() - dolState.getDolProducedTimestamp();
  LOGGER.info(
      "Ready to switch to leader topic for partition {}: DoL loopback confirmed (latency: {}ms, term: {})",
      pcs.getPartition(),
      loopbackLatency,
      dolState.getLeadershipTerm());
  
  return true;
}
```

### 4. DoL Message Production

Produce DoL message when entering `IN_TRANSITION_FROM_STANDBY_TO_LEADER` state:

```java
/**
 * Produces a Declaration of Leadership (DoL) message to the local VT.
 * This is called when transitioning from STANDBY to LEADER state.
 * 
 * @param pcs partition consumption state
 */
private void produceDeclarationOfLeadership(PartitionConsumptionState pcs) {
  DeclarationOfLeadershipState dolState = pcs.getDolState();
  
  // Avoid duplicate production
  if (dolState.isDolMessageProduced()) {
    LOGGER.warn(
        "DoL message already produced for partition {}, skipping duplicate production",
        pcs.getPartition());
    return;
  }
  
  // Generate leadership term (can use timestamp or counter)
  long leadershipTerm = System.currentTimeMillis();
  dolState.setLeadershipTerm(leadershipTerm);
  
  // Create DoL message using START_OF_SEGMENT with special metadata
  StartOfSegment sosPayload = new StartOfSegment();
  sosPayload.isDeclarationOfLeadership = true;  // New field
  sosPayload.leadershipTerm = leadershipTerm;   // New field
  sosPayload.checksumType = CheckSumType.NONE.getValue();
  
  // Wrap in control message
  ControlMessage controlMessage = new ControlMessage();
  controlMessage.controlMessageType = ControlMessageType.START_OF_SEGMENT.getValue();
  controlMessage.controlMessageUnion = sosPayload;
  
  // Produce to local VT with DECLARATION_OF_LEADERSHIP key
  int partition = pcs.getPartition();
  VeniceWriter<KafkaKey, byte[], byte[]> writer = getVeniceWriter(pcs);
  
  try {
    long producedTimestamp = System.currentTimeMillis();
    dolState.setDolProducedTimestamp(producedTimestamp);
    
    CompletableFuture<PubSubProduceResult> produceFuture = 
        writer.sendControlMessage(
            KafkaKey.DECLARATION_OF_LEADERSHIP,
            controlMessage,
            partition,
            new DolProduceCallback(pcs, leadershipTerm, producedTimestamp));
    
    dolState.setDolProduceFuture(produceFuture);
    dolState.setDolMessageProduced(true);
    
    LOGGER.info(
        "Produced DoL message to local VT for partition {} with term {}",
        partition,
        leadershipTerm);
        
  } catch (Exception e) {
    dolState.reset();
    throw new VeniceException(
        "Failed to produce DoL message for partition " + partition,
        e);
  }
}

/**
 * Callback to handle DoL message produce completion.
 */
private class DolProduceCallback implements PubSubProducerCallback {
  private final PartitionConsumptionState pcs;
  private final long leadershipTerm;
  private final long producedTimestamp;
  
  public DolProduceCallback(
      PartitionConsumptionState pcs,
      long leadershipTerm,
      long producedTimestamp) {
    this.pcs = pcs;
    this.leadershipTerm = leadershipTerm;
    this.producedTimestamp = producedTimestamp;
  }
  
  @Override
  public void onCompletion(PubSubProduceResult produceResult, Exception exception) {
    if (exception != null) {
      LOGGER.error(
          "Failed to produce DoL message for partition {} with term {}",
          pcs.getPartition(),
          leadershipTerm,
          exception);
      pcs.getDolState().reset();
      return;
    }
    
    // Record the position where DoL was produced
    PubSubPosition dolPosition = produceResult.getPosition();
    pcs.getDolState().setDolPosition(dolPosition);
    
    LOGGER.info(
        "DoL message produce confirmed for partition {} at position {} (term: {})",
        pcs.getPartition(),
        dolPosition,
        leadershipTerm);
  }
}
```

### 5. DoL Message Consumption Detection

Detect when the DoL message is consumed back:

```java
/**
 * Checks if the consumed control message is the DoL message we're waiting for.
 * Called during control message processing in the consumption path.
 * 
 * @param consumerRecord the consumed record
 * @param controlMessage the control message payload
 * @param pcs partition consumption state
 * @return true if this is the DoL message we're waiting for
 */
private boolean checkAndHandleDeclarationOfLeadership(
    DefaultPubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> consumerRecord,
    ControlMessage controlMessage,
    PartitionConsumptionState pcs) {
    
  // Only check during transition to leader
  if (pcs.getLeaderFollowerState() != IN_TRANSITION_FROM_STANDBY_TO_LEADER) {
    return false;
  }
  
  // Must be START_OF_SEGMENT with DECLARATION_OF_LEADERSHIP key
  if (ControlMessageType.valueOf(controlMessage) != START_OF_SEGMENT) {
    return false;
  }
  
  if (!Arrays.equals(consumerRecord.getKey().getKey(), KafkaKey.DECLARATION_OF_LEADERSHIP.getKey())) {
    return false;
  }
  
  // Check if this is a DoL message (verify the flag is set)
  StartOfSegment sos = (StartOfSegment) controlMessage.controlMessageUnion;
  if (!sos.isDeclarationOfLeadership) {
    return false;  // Not a DoL message, possibly malformed
  }
  
  // Verify this is OUR DoL message (match leadership term)
  DeclarationOfLeadershipState dolState = pcs.getDolState();
  if (sos.leadershipTerm != dolState.getLeadershipTerm()) {
    LOGGER.warn(
        "Consumed DoL message with mismatched term for partition {}: expected {}, got {}",
        pcs.getPartition(),
        dolState.getLeadershipTerm(),
        sos.leadershipTerm);
    return false;
  }
  
  // Loopback confirmed!
  long consumedTimestamp = System.currentTimeMillis();
  dolState.setDolConsumed(true);
  dolState.setDolConsumedTimestamp(consumedTimestamp);
  
  long loopbackLatency = consumedTimestamp - dolState.getDolProducedTimestamp();
  LOGGER.info(
      "DoL loopback confirmed for partition {}: term={}, loopback_latency={}ms, position={}",
      pcs.getPartition(),
      sos.leadershipTerm,
      loopbackLatency,
      consumerRecord.getPosition());
  
  // Emit metrics for monitoring
  emitDolLoopbackMetric(pcs.getPartition(), loopbackLatency);
  
  return true;
}
```

### 6. Integration into State Transition Logic

Update the state transition flow to use DoL:

```java
// In checkConsumptionStateForStoreMetadataChanges() method

case IN_TRANSITION_FROM_STANDBY_TO_LEADER:
  // NEW: Produce DoL message if not already produced
  DeclarationOfLeadershipState dolState = partitionConsumptionState.getDolState();
  if (!dolState.isDolMessageProduced()) {
    produceDeclarationOfLeadership(partitionConsumptionState);
  }
  
  // Check if we can switch to leader topic (now uses DoL confirmation)
  if (canSwitchToLeaderTopic(partitionConsumptionState)) {
    LOGGER.info(
        "Initiating promotion of replica: {} to leader for the partition. Unsubscribing from the current topic: {}",
        partitionConsumptionState.getReplicaId(),
        kafkaVersionTopic);
    
    // Unsubscribe from VT and switch to RT...
    // (existing logic continues)
    
    // IMPORTANT: Reset DoL state after successful transition
    dolState.reset();
  }
  break;

// In processControlMessage() method
case START_OF_SEGMENT:
  // Check if this is a DoL message we're waiting for
  if (checkAndHandleDeclarationOfLeadership(consumerRecord, controlMessage, partitionConsumptionState)) {
    // DoL loopback confirmed - canSwitchToLeaderTopic() will now return true
  }
  // ... existing START_OF_SEGMENT handling ...
  break;
```

### 7. State Reset and Cleanup

Reset DoL state in appropriate scenarios:

```java
// When transitioning back to STANDBY or on partition unsubscribe
private void resetDeclarationOfLeadershipState(PartitionConsumptionState pcs) {
  pcs.getDolState().reset();
  LOGGER.debug("Reset DoL state for partition {}", pcs.getPartition());
}

// Call in:
// 1. LEADER -> STANDBY transition
// 2. Partition unsubscribe
// 3. Ingestion task shutdown
// 4. Error handling paths
```

---

## Schema Changes

### 1. Create v14 of KafkaMessageEnvelope and add fields to StartOfSegment

**Path**: `/internal/venice-common/src/main/resources/avro/KafkaMessageEnvelope/v14/KafkaMessageEnvelope.avsc`

Copy v13 and add these two fields to the `StartOfSegment` record (within the `controlMessageUnion`):

```json
{
  "name": "StartOfSegment",
  "type": "record",
  "fields": [
    {
      "name": "checksumType",
      "type": "int"
    }, {
      "name": "upcomingAggregates",
      "type": {
        "type": "array",
        "items": "string"
      }
    }, {
      "name": "isDeclarationOfLeadership",
      "type": "boolean",
      "default": false,
      "doc": "Flag indicating this START_OF_SEGMENT is a Declaration of Leadership (DoL) message for fast leader handover"
    }, {
      "name": "leadershipTerm",
      "type": "long",
      "default": 0,
      "doc": "Leadership term/epoch for DoL messages. Used to match produced and consumed DoL messages during loopback"
    }
  ]
}
```

These are **optional fields with defaults**, ensuring **backward compatibility** with existing v13 producers/consumers.

After adding the schema, update the protocol version mapping in the codebase to register v14.

### 2. Add new constant to `KafkaKey.java`:

```java
/**
 * Special key for Declaration of Leadership (DoL) control messages.
 * Used during leader handover to confirm the new leader can write to and consume from the local VT.
 * This is distinct from HEART_BEAT to clearly separate leadership declaration from heartbeat semantics.
 */
public static final KafkaKey DECLARATION_OF_LEADERSHIP = new KafkaKey(
    MessageType.CONTROL_MESSAGE,
    ByteBuffer.allocate(CONTROL_MESSAGE_KAFKA_KEY_LENGTH)
        .put(DolGuidGenerator.getInstance().getGuid().bytes())  // Use distinct GUID generator
        .putInt(0)  // segment number
        .putInt(0)  // sequence number
        .array());
```

**Note**: Create a dedicated `DolGuidGenerator` similar to `HeartbeatGuidV3Generator` to ensure DoL messages have a distinct, recognizable GUID pattern.

---

## Configuration

The DoL mechanism is controlled by **two separate configuration flags** to enable independent rollout for system stores and user stores:

### 1. System Stores Configuration

**Config Key**: `server.leader.handover.use.dol.mechanism.for.system.stores`

**Purpose**: Controls DoL mechanism for system stores (meta stores, push status stores, etc.)

**Default**: `false` (uses legacy time-based mechanism)

**Usage in VeniceServerConfig**:
```java
private final boolean leaderHandoverUseDoLMechanismForSystemStores;

public VeniceServerConfig(VeniceProperties serverProperties) {
  this.leaderHandoverUseDoLMechanismForSystemStores =
      serverProperties.getBoolean(SERVER_LEADER_HANDOVER_USE_DOL_MECHANISM_FOR_SYSTEM_STORES, false);
}

public boolean isLeaderHandoverUseDoLMechanismEnabledForSystemStores() {
  return this.leaderHandoverUseDoLMechanismForSystemStores;
}
```

### 2. User Stores Configuration

**Config Key**: `server.leader.handover.use.dol.mechanism.for.user.stores`

**Purpose**: Controls DoL mechanism for user stores (regular application data stores)

**Default**: `false` (uses legacy time-based mechanism)

**Usage in VeniceServerConfig**:
```java
private final boolean leaderHandoverUseDoLMechanismForUserStores;

public VeniceServerConfig(VeniceProperties serverProperties) {
  this.leaderHandoverUseDoLMechanismForUserStores =
      serverProperties.getBoolean(SERVER_LEADER_HANDOVER_USE_DOL_MECHANISM_FOR_USER_STORES, false);
}

public boolean isLeaderHandoverUseDoLMechanismEnabledForUserStores() {
  return this.leaderHandoverUseDoLMechanismForUserStores;
}
```

### 3. Config Selection Logic

The `canSwitchToLeaderTopic()` method selects the appropriate config based on store type:

```java
private boolean canSwitchToLeaderTopic(PartitionConsumptionState pcs) {
  // Check if DoL mechanism is enabled via config (system stores vs user stores)
  boolean useDoLMechanism = isIngestingSystemStore()
      ? serverConfig.isLeaderHandoverUseDoLMechanismEnabledForSystemStores()
      : serverConfig.isLeaderHandoverUseDoLMechanismEnabledForUserStores();

  if (useDoLMechanism) {
    // Use DoL-based logic
    return checkDoLLoopbackConfirmation(pcs);
  } else {
    // Use legacy time-based mechanism
    return canSwitchToLeaderTopicLegacy(pcs);
  }
}
```

### 4. Rollout Strategy

Having separate configs enables a **phased rollout approach**:

**Phase 1: System Stores First**
- Set `server.leader.handover.use.dol.mechanism.for.system.stores=true`
- System stores typically have lower volume and are easier to monitor
- Validate DoL mechanism works correctly on critical infrastructure stores

**Phase 2: User Stores Rollout**
- After system stores are stable, enable for user stores
- Set `server.leader.handover.use.dol.mechanism.for.user.stores=true`
- Can roll out gradually by store or by region

**Phase 3: Full Adoption**
- Both configs enabled cluster-wide
- Monitor metrics to ensure improved handover latency
- Eventual deprecation of legacy time-based mechanism

### 5. Benefits of Separate Configs

- **Risk Mitigation**: Test on critical system stores before wider user store rollout
- **Independent Control**: Disable DoL for user stores without affecting system stores (or vice versa)
- **Gradual Migration**: Roll out to different store types at different paces
- **Operational Flexibility**: Quick rollback per store type if issues arise
- **A/B Testing**: Compare DoL vs. legacy behavior for different store types

---

## Benefits of DoL Approach

### 1. **Deterministic Confirmation**
- Explicit proof that new leader can write to and read from VT
- No reliance on timing assumptions or probabilistic waits

### 2. **Fast Handover**
- Loopback latency typically milliseconds (vs. minutes with time-based approach)
- Eliminates unnecessary `newLeaderInactiveTime` wait period

### 3. **Strong Consistency Guarantee**
- New leader has confirmed its position in VT before consuming RT
- Prevents split-brain scenarios where old/new leaders overlap
- **Consuming DoL back is inherent confirmation of being fully caught up on VT**
- Eliminates need for special-case logic (e.g., `isUserSystemStore()` checks)

### 4. **Unified Logic Across Store Types**
- DoL loopback confirmation means the leader has processed all VT messages up to its DoL
- No need for separate `isLocalVersionTopicPartitionFullyConsumed()` checks for user system stores
- Single, consistent handover mechanism for all stores (user stores, system stores, hybrid stores)
- Simplifies code and reduces maintenance burden

### 5. **Observability**
- DoL loopback latency metric tracks handover speed
- Clear state tracking for debugging and monitoring
- Explicit log messages for handover progress

### 5. **Graceful Degradation**
- If DoL message never loops back (e.g., VT issues), state transition won't proceed
- Prevents unsafe promotion to leader

### 6. **Backward Compatible**
- New DoL fields are optional in `StartOfSegment`
- Existing replicas ignore DoL messages (see them as regular heartbeats)
- Gradual rollout possible

---

## Metrics and Monitoring

Add metrics to track DoL mechanism:

```java
// DoL loopback latency histogram
hostLevelIngestionStats.recordDolLoopbackLatency(storeName, partition, loopbackLatency);

// DoL state gauges
hostLevelIngestionStats.recordDolProduced(storeName, partition, 1);
hostLevelIngestionStats.recordDolConsumed(storeName, partition, 1);

// DoL failures counter
hostLevelIngestionStats.recordDolFailure(storeName, partition, reason);
```

---

## Testing Strategy

### Unit Tests
1. Test DoL state transitions
2. Test DoL message production
3. Test DoL message consumption detection
4. Test term matching logic
5. Test state reset scenarios

### Integration Tests
1. Leader handover with DoL mechanism end-to-end
2. Multiple concurrent leader transitions
3. DoL message loss scenarios (retry/timeout)
4. Backward compatibility with old replicas

### Stress Tests
1. Rapid leader failover cycles
2. High partition count scenarios
3. Network partition during DoL loopback

---

## Migration and Rollout

### Phase 1: Schema Evolution
- Deploy `StartOfSegment` schema with new optional fields (`isDeclarationOfLeadership`, `leadershipTerm`)
- Add `KafkaKey.DECLARATION_OF_LEADERSHIP` constant and `DolGuidGenerator`
- No behavior changes yet - configs remain `false` by default

### Phase 2: DoL Code Deployment
- Deploy DoL production and consumption logic
- Both configs (`for.system.stores` and `for.user.stores`) default to `false`
- Code is in place but not activated

### Phase 3: Enable DoL for System Stores (Canary)
- Set `server.leader.handover.use.dol.mechanism.for.system.stores=true` on canary servers
- Monitor DoL loopback metrics for system stores (meta stores, push status stores)
- Validate DoL messages are produced and consumed correctly
- User stores continue using legacy time-based mechanism

### Phase 4: System Stores Full Rollout
- After canary validation, enable DoL for all system stores cluster-wide
- Set `server.leader.handover.use.dol.mechanism.for.system.stores=true` globally
- Monitor for 1-2 weeks to ensure stability

### Phase 5: Enable DoL for User Stores (Gradual)
- Start with low-traffic user stores or specific regions
- Set `server.leader.handover.use.dol.mechanism.for.user.stores=true` gradually
- Monitor handover latency improvements and error rates
- Expand to high-traffic stores after validation

### Phase 6: Full Adoption
- Both configs enabled cluster-wide
- System stores and user stores using DoL mechanism
- Legacy time-based logic remains as fallback in code

### Phase 7: Cleanup (Future)
- After 6-12 months of stable operation, consider removing legacy code
- Deprecate and remove `canSwitchToLeaderTopicLegacy()` method
- Remove time-based fallback logic

---

## Edge Cases and Error Handling

### 1. DoL Message Lost
- **Symptom**: DoL produced but never consumed back
- **Handling**: Add timeout (e.g., 2 × `newLeaderInactiveTime`)
- **Recovery**: Retry DoL production or fall back to time-based check

### 2. Concurrent Leader Transitions
- **Symptom**: Multiple DoL messages with different terms
- **Handling**: Term matching ensures we only confirm OUR DoL
- **Recovery**: Reset on state transitions, only track latest term

### 3. VT Write Failures
- **Symptom**: DoL produce operation fails
- **Handling**: Reset DoL state, retry or escalate error
- **Recovery**: Don't transition to LEADER without DoL confirmation

### 4. Partition Reassignment During DoL
- **Symptom**: Partition unsubscribed while waiting for DoL
- **Handling**: Reset DoL state on unsubscribe
- **Recovery**: New state model invocation starts fresh DoL cycle

### 5. Old Leader Still Producing
- **Symptom**: Old leader continues after new leader DoL
- **Handling**: Term/GUID in DoL helps identify producer
- **Recovery**: Followers detect via changed producer metadata

---

## Alternative Considered: Explicit DoL Control Message Type

Instead of reusing `START_OF_SEGMENT`, we could create a new `ControlMessageType.DECLARATION_OF_LEADERSHIP`.

**Pros:**
- Clearer semantic meaning
- Dedicated schema without mixing concerns

**Cons:**
- Requires new control message type (more schema evolution)
- Less reuse of existing infrastructure
- More code changes across consumers

**Decision**: Use `START_OF_SEGMENT` with special flag for simpler implementation and better reuse.

---

## Summary

The Declaration of Leadership (DoL) mechanism provides a **deterministic, fast, and reliable** approach to leader handover by:

1. **Loopback Confirmation**: New leader produces DoL message to VT and waits to consume it back
2. **Explicit State Tracking**: `DeclarationOfLeadershipState` in `PartitionConsumptionState` tracks DoL lifecycle
3. **Deterministic Verification**: Replaces unreliable time-based waits with position-based verification
4. **Unified Logic**: Same handover mechanism for all store types (no special `isUserSystemStore()` checks)
5. **Backward Compatibility**: Optional schema fields with defaults, dedicated `KafkaKey.DECLARATION_OF_LEADERSHIP`

### Key Implementation Components

**Schema/Protocol Changes:**
- Add `isDeclarationOfLeadership` and `leadershipTerm` fields to `StartOfSegment.avsc`
- Add `KafkaKey.DECLARATION_OF_LEADERSHIP` constant with dedicated GUID generator
- Add `DeclarationOfLeadershipState` class to track DoL lifecycle

**Core Logic Changes:**
- `produceDeclarationOfLeadership()`: Produces DoL message when entering `IN_TRANSITION_FROM_STANDBY_TO_LEADER`
- `checkAndHandleDeclarationOfLeadership()`: Detects DoL message consumption with term matching
- `canSwitchToLeaderTopic()`: Simplified to check only DoL confirmation (no time-based or store-type logic)

**Benefits:**
- ⚡ **Millisecond handover** vs. minutes with time-based approach
- 🎯 **Deterministic** confirmation of VT write/read capability and caught-up state
- 🔒 **Safe** prevention of split-brain scenarios
- 📊 **Observable** with clear metrics and state tracking
- 🧹 **Simplified** codebase with unified logic for all store types

This design eliminates the race conditions and timing dependencies of the current implementation while providing clear observability into the handover process.
