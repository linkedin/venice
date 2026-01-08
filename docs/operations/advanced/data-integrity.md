

# Venice Data Integrity Validation

### What is Data Integrity Validation (DIV)?
- Validate and detect errors in transmitted (Kafka) data.
- The sender (VeniceWriter) calculates and includes the DIV metadata.
- The receiver recalculates and verifies the DIV metadata on arrival.

### What does DIV metadata look like?
- Sender transmits data in segments.
- Sender decides when to close the current segment and start a new one.
- A segment contains multiple Kafka records
  * Start of Segment (SOS)
  * End of Segment (EOS)
  * Control or data messages between
- GUID
  * One unique ID per-sender (VeniceWriter).
  * GUID changes if VeniceWriter or the server restarts.
- Sequence number
  * Within a segment, the sequence number starts at 0.

### Data integrity issues in Venice:
- A segment starts from a non-zero sequence number (UNREGISTERED_PRODUCER).
- A gap exists between or within segments (MISSING).
- Data within a segment is corrupted (CORRUPT).
- Producers have produced duplicate messages, which is expected due to retries (DUPLICATE).

### Different behaviors before and after End Of Push (EOP)
- End Of Push (EOP) is a control message in the Kafka topic sent once per partition at bulk load end, after all data producers come online.
- When pushing a new store version, if DIV detects fatal data validation issues (including UNREGISTERED_PRODUCER, MISSING, and CORRUPT) while consuming batch push data before EOP, the ingestion task errors and aborts.
- If issues appear after receiving EOP, the ingestion task continues, but DIV logs a warning about data integrity issues.

### Data structures
- Sender - VeniceWriter/Segment
- Data - ProducerRecord/KafkaMessageEnvelope/ProducerMetadata
- Receiver - ConsumptionTask/StoreIngestionTask

### Persistence
- DIV needs to be checkpointed for several reasons:
  * DIV state would be lost on server restarts leading to potential data integrity issues undetected (inaccuracy)
  * Otherwise, without checkpointing, DIV needs to rebuild its state from the beginning of the topic on every restart (inefficiency)
  * In case of a crash or restart, we need to know where to resume validation from (last checkpoint).

### Why Two Separate DIV validators?
- Two state pipeline
- Stage 1: Consumer Thread
  * Kafka → Consumer Thread → Validation → Queue → ...
  * Reads messages from Kafka topics
  * Performs validation using Consumer DIV (RT or remote VT)
  * Queues messages to StoreBufferService

- Stage 2: Drainer Thread
  * ... → Queue → Drainer Thread → Validation → Storage Engine
  * Dequeues messages from StoreBufferService
  * Performs validation using drainer DIV (all topics)
  * Persists data to storage engine
  * Checkpoints offsets to disk.

- Consumer is always ahead of drainer
  * The consumer thread reads from Kafka faster than the drainer can persist to disk. There's buffering in between.
  * Kafka producer buffers (for leaders)
  * StoreBufferService queue etc.
  * Time T1: Consumer validates message at offset 1000 → Consumer DIV state = offset 1000
  * Time T2: Consumer validates message at offset 2000 → Consumer DIV state = offset 2000
  * Time T3: Drainer persists message at offset 1000 → Drainer DIV state = offset 1000
  * If we only had one consumer DIV, we couldn't checkpoint the correct state to disk.

- DIV State Must Match Persisted Data
  * The DIV checkpoint saved to disk must exactly match what's actually persisted in the storage engine. Since the drainer is responsible for persistence, only the Drainer DIV state can be safely checkpointed.

- Leader Double Validation
  * Consumer DIV (in consumer thread): Validates RT messages before producing to VT
  * Drainer DIV (in drainer thread): Validates same messages again before persisting (unnecessary)

### DIV in State Transitions (per-partition)
- **OFFLINE -> STANDBY:** DIV state restoration.
  The DIV state is restored from the persisted OffsetRecord (drainerDiv). In STANDBY mode, the DIV continues to validate incoming messages against the restored state and keeps it updated.
- **STANDBY -> OFFLINE:** The DIV state is cleared immediately without being persisted to disk.
  Any DIV state accumulated since the last checkpoint is lost The system relies on periodic checkpoints during consumption, not on-demand checkpoints during state transitions. This design choice prioritizes fast un-subscription over preserving the absolute latest DIV state.
- **STANDBY -> LEADER:** Wipes all DIV state for the partition in the consumer DIV.
  Copies the producer states from the drainer's DIV validator to the consumer DIV.
- **LEADER -> STANDBY:** The drainer DIV maintains producer states that have been validated and persisted.
  If this replica becomes leader again, it will clone the drainer DIV state.

### Kafka Log Compaction impacts DIV in Venice
- When Kafka log compaction runs, it:
  * Deletes duplicate records (identified by the key) and keeps only the latest record.
  * Creates **gaps** in sequence numbers that DIV would normally flag as data loss.
  * Venice uses a **log compaction delay threshold** to distinguish between:
    * **Real data loss** (missing messages within the compaction window)
    * **Expected gaps** (missing messages beyond the compaction window)
  * Example:
    * T0: Producer sends messages with seq# 1, 2, 3, 4, 5
    * T1: Kafka stores all messages
    * T2: min.compaction.lag.ms = 24 hours passes
    * T3: Kafka compaction runs, deletes messages 2, 3 (duplicate keys)
    * T4: Consumer reads: seq# 1, 4, 5 (gap detected!)
  * DIV Check:
    * Last message timestamp: T0
    * Current time: T4 (> 24 hours later)
    * elapsedTime >= min.compaction.lag.ms? YES
  * Result: TOLERATE the gap (expected compaction)
    * T4: Consumer reads: seq# 1, 4, 5 (gap detected!)
    * elapsedTime < min.compaction.lag.ms? YES
  * Result: THROW EXCEPTION (data loss!)

### DIV cleanup mechanism
- Prevents DIV from accumulating unbounded producer state by automatically expiring and removing old producer tracking information.
- Without cleanup, this state grows indefinitely
  * Large memory footprint - State accumulates in heap
  * Checkpoint overhead - All producer states are persisted to disk on every offset sync
- Solution: Time-Based Expiration
  * **MaxAge** allows Venice to automatically expire old producer state that's no longer relevant.
  * Only applied to drainer DIV (not consumer DIV, which is transient)
  * For hybrid stores, maxAge is >= rewind time.
  * Can be disabled by setting to -1.
- How it works:
  * When checkpointing offsets, DIV clears expired state.
  * When loading state from disk (e.g., after restart), maxAge is also applied. This prevents loading stale state after a restart.
  * Why MaxAge ≥ Rewind Time for Hybrid Stores?
    * Support RT replay, otherwise it doesn't recognize producer’s div state in RT.