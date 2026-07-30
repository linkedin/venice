package com.linkedin.venice.hadoop.task.datawriter;

import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriterHook;
import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;


/**
 * Optional VPJ executor-side provider for the hook attached to the primary data {@code VeniceWriter}.
 *
 * <p>Implementations must have a public no-arg constructor. VPJ creates one provider per partition writer,
 * invokes {@link #createWriterHook(Context)} exactly once, and keeps the provider open until the partition
 * writer has finished flushing and closing its Venice writers. The provider owns the lifecycle of any
 * resources used by the returned hook and must release them from {@link #close()}.
 *
 * <p>The hook is attached only to the primary data writer. It is not attached to control-message,
 * heartbeat, or materialized-view child writers.
 */
public interface VeniceWriterHookProvider extends Closeable {
  VeniceWriterHook createWriterHook(Context context);

  @Override
  default void close() throws IOException {
    // No-op by default for providers without resources.
  }

  /**
   * Immutable executor initialization context for a VPJ partition writer.
   */
  final class Context {
    private final VeniceProperties jobProperties;
    private final String storeName;
    private final String topicName;
    private final String jobName;
    private final int taskId;
    private final int partitionCount;

    public Context(
        VeniceProperties jobProperties,
        String storeName,
        String topicName,
        String jobName,
        int taskId,
        int partitionCount) {
      this.jobProperties = Objects.requireNonNull(jobProperties, "jobProperties");
      this.storeName = Objects.requireNonNull(storeName, "storeName");
      this.topicName = Objects.requireNonNull(topicName, "topicName");
      this.jobName = jobName;
      this.taskId = taskId;
      this.partitionCount = partitionCount;
    }

    public VeniceProperties getJobProperties() {
      return jobProperties;
    }

    public String getStoreName() {
      return storeName;
    }

    public String getTopicName() {
      return topicName;
    }

    public String getJobName() {
      return jobName;
    }

    /**
     * The compute-engine task ID. For a VPJ partition writer this is also the destination partition ID.
     */
    public int getTaskId() {
      return taskId;
    }

    public int getPartitionCount() {
      return partitionCount;
    }
  }
}
