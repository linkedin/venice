package com.linkedin.venice.hadoop.snapshot;

import com.linkedin.davinci.replication.RmdWithValueSchemaId;
import com.linkedin.davinci.replication.merge.MergeConflictResolver;
import com.linkedin.davinci.replication.merge.MergeConflictResolverFactory;
import com.linkedin.davinci.replication.merge.MergeConflictResult;
import com.linkedin.davinci.replication.merge.RmdSerDe;
import com.linkedin.davinci.replication.merge.StringAnnotatedStoreSchemaCache;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.rmd.RmdConstants;
import com.linkedin.venice.utils.lazy.Lazy;
import java.nio.ByteBuffer;


/**
 * Offline merge of real-time (RT) writes onto a batch base value, producing the merged value <b>and its
 * Replication Metadata (RMD)</b>, by reusing Venice's own {@link MergeConflictResolver}. The result is
 * identical to what a server would produce by replaying the same RT records on top of the batch data
 * during a hybrid rewind.
 *
 * <p>This is the correctness core of the "snapshot-at-T" data plane. Instead of every server replica
 * replaying RT after a batch push (the long hybrid rewind), the push job folds RT up to a cutoff {@code T}
 * into the batch dataset once, offline. Because the merged record carries real per-record / per-field RMD,
 * the short {@code [T, now]} rewind that follows resolves correctly against it. Reusing
 * {@link MergeConflictResolver} (rather than re-implementing conflict resolution) is what makes the offline
 * result equal to the online one — including Active-Active field-level timestamps and write-compute partial
 * updates.
 *
 * <p>Per key: {@link #seedFromBatch} with the offline batch value (which establishes the batch-sentinel RMD,
 * timestamp {@link RmdConstants#BATCH_RMD_SENTINEL_TIMESTAMP}, exactly as a server does for batch data, so any
 * real RT write applied next wins), or {@link #newState} for an RT-only key; then
 * {@link #applyPut}/{@link #applyUpdate}/{@link #applyDelete} for each RT record; then {@link #finalizeRecord}.
 * Conflict resolution is timestamp-commutative, so the converged result does not depend on the order RT
 * records are folded in.
 *
 * <p>Not thread-safe. Use one instance and one {@link KeyMergeState} per key within a task.
 */
public class SnapshotAtTRecordMerger {
  private final MergeConflictResolver resolver;
  private final RmdSerDe rmdSerDe;
  private final int rmdProtocolVersion;

  /**
   * @param schemaRepository resolves value, RMD, and write-compute (derived) schemas for the store
   * @param storeName the Venice store name
   * @param rmdProtocolVersion the store's RMD protocol version (the version the merged RMD is written with)
   * @param rmdUseFieldLevelTimestamp {@code true} for stores that track per-field timestamps (write-compute /
   *        partial-update stores); {@code false} for value-level timestamps. Must match the store's config so
   *        the offline merge matches the server.
   */
  public SnapshotAtTRecordMerger(
      ReadOnlySchemaRepository schemaRepository,
      String storeName,
      int rmdProtocolVersion,
      boolean rmdUseFieldLevelTimestamp) {
    StringAnnotatedStoreSchemaCache schemaCache = new StringAnnotatedStoreSchemaCache(storeName, schemaRepository);
    this.rmdSerDe = new RmdSerDe(schemaCache, rmdProtocolVersion);
    this.resolver = MergeConflictResolverFactory.getInstance()
        .createMergeConflictResolver(schemaCache, rmdSerDe, storeName, rmdUseFieldLevelTimestamp, true);
    this.rmdProtocolVersion = rmdProtocolVersion;
  }

  /** Mutable merge state for a single key. */
  public static final class KeyMergeState {
    // Current value (no value-schema-id prefix), or null once the key resolves to a delete / before any value
    // has been established. Stored as a byte[] so it is independent of any ByteBuffer position the resolver may
    // have advanced while reading the inputs.
    private byte[] valueBytes;
    private int valueSchemaId;
    // Current RMD; null until the first value is established (batch seed or first RT write), after which the
    // resolver always supplies one.
    private RmdWithValueSchemaId rmd;

    private KeyMergeState(int valueSchemaId) {
      this.valueSchemaId = valueSchemaId;
    }

    /** The merged value bytes (no schema-id prefix), or {@code null} if the key resolves to a delete. */
    public ByteBuffer getValueBytes() {
      return valueBytes == null ? null : ByteBuffer.wrap(valueBytes);
    }

    public int getValueSchemaId() {
      return valueSchemaId;
    }

    public boolean isDeleted() {
      return valueBytes == null;
    }
  }

  /** A finished merged record, ready to be written to the new version topic. */
  public static final class MergedRecord {
    private final ByteBuffer value;
    private final ByteBuffer rmd;
    private final int valueSchemaId;
    private final int rmdProtocolVersion;
    private final boolean delete;

    MergedRecord(ByteBuffer value, ByteBuffer rmd, int valueSchemaId, int rmdProtocolVersion, boolean delete) {
      this.value = value;
      this.rmd = rmd;
      this.valueSchemaId = valueSchemaId;
      this.rmdProtocolVersion = rmdProtocolVersion;
      this.delete = delete;
    }

    /** Merged value bytes (no schema-id prefix), or {@code null} for a delete. */
    public ByteBuffer getValue() {
      return value;
    }

    /** Serialized merged RMD. Always present (a delete still carries an RMD tombstone). */
    public ByteBuffer getRmd() {
      return rmd;
    }

    public int getValueSchemaId() {
      return valueSchemaId;
    }

    public int getRmdProtocolVersion() {
      return rmdProtocolVersion;
    }

    public boolean isDelete() {
      return delete;
    }
  }

  /** A fresh state for a key that has no batch value (RT-only); the first applied RT write establishes it. */
  public KeyMergeState newState(int valueSchemaId) {
    return new KeyMergeState(valueSchemaId);
  }

  /**
   * Seed the merge for a key from its offline batch value. Applying it with no prior RMD yields the
   * batch-sentinel RMD (timestamp {@link RmdConstants#BATCH_RMD_SENTINEL_TIMESTAMP}), matching how a server
   * stores batch data, so any real RT write folded in afterwards wins.
   */
  public KeyMergeState seedFromBatch(ByteBuffer batchValueBytes, int valueSchemaId) {
    KeyMergeState state = new KeyMergeState(valueSchemaId);
    applyPut(state, batchValueBytes, valueSchemaId, RmdConstants.BATCH_RMD_SENTINEL_TIMESTAMP, 0);
    return state;
  }

  /** Fold a full-value PUT from RT. */
  public void applyPut(KeyMergeState state, ByteBuffer value, int valueSchemaId, long writeTimestamp, int coloId) {
    MergeConflictResult result =
        resolver.put(oldValueProvider(state), state.rmd, value, writeTimestamp, valueSchemaId, coloId);
    applyResult(state, result);
  }

  /** Fold a write-compute (partial-update) UPDATE from RT. */
  public void applyUpdate(
      KeyMergeState state,
      ByteBuffer updateBytes,
      int valueSchemaId,
      int updateProtocolVersion,
      long writeTimestamp,
      int coloId) {
    MergeConflictResult result = resolver.update(
        oldValueProvider(state),
        state.rmd,
        updateBytes,
        valueSchemaId,
        updateProtocolVersion,
        writeTimestamp,
        coloId,
        null);
    applyResult(state, result);
  }

  /** Fold a DELETE from RT. */
  public void applyDelete(KeyMergeState state, long writeTimestamp, int coloId) {
    MergeConflictResult result = resolver.delete(oldValueProvider(state), state.rmd, writeTimestamp, coloId);
    applyResult(state, result);
  }

  /** Produce the writer-ready merged record (value + serialized RMD) for the current state. */
  public MergedRecord finalizeRecord(KeyMergeState state) {
    ByteBuffer rmdBytes = rmdSerDe.serializeRmdRecord(state.valueSchemaId, state.rmd.getRmdRecord());
    return new MergedRecord(
        state.getValueBytes(),
        rmdBytes,
        state.valueSchemaId,
        rmdProtocolVersion,
        state.isDeleted());
  }

  private static Lazy<ByteBuffer> oldValueProvider(KeyMergeState state) {
    // A fresh ByteBuffer per call so a previously-advanced position can never leak into a later operation.
    return Lazy.of(() -> state.valueBytes == null ? null : ByteBuffer.wrap(state.valueBytes));
  }

  private void applyResult(KeyMergeState state, MergeConflictResult result) {
    // A stale write (older than what is already merged) is ignored, leaving the state untouched.
    if (result.isUpdateIgnored()) {
      return;
    }
    state.valueBytes = toByteArray(result.getNewValue());
    state.valueSchemaId = result.getValueSchemaId();
    state.rmd = new RmdWithValueSchemaId(result.getValueSchemaId(), rmdProtocolVersion, result.getRmdRecord());
  }

  /** Read a ByteBuffer's full content into a byte[] without depending on (or mutating) its position. */
  private static byte[] toByteArray(ByteBuffer buffer) {
    if (buffer == null) {
      return null;
    }
    ByteBuffer duplicate = buffer.duplicate();
    duplicate.rewind();
    byte[] bytes = new byte[duplicate.remaining()];
    duplicate.get(bytes);
    return bytes;
  }
}
