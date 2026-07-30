package com.linkedin.venice.hadoop.task.datawriter;

import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriterHook;
import java.util.Objects;


/**
 * Optional VPJ executor-side factory for the hook attached to the primary data {@code VeniceWriter}.
 *
 * <p>Implementations must have a public no-arg constructor. VPJ creates one factory per partition writer and
 * invokes {@link #createWriterHook(Context)} exactly once during executor task initialization.
 *
 * <p>The hook is attached only to the primary data writer. It is not attached to control-message,
 * heartbeat, or materialized-view child writers.
 */
public interface VeniceWriterHookFactory {
  VeniceWriterHook createWriterHook(Context context);

  /**
   * Immutable executor initialization context for a VPJ partition writer.
   */
  final class Context {
    private final VeniceProperties taskProperties;
    private final String storeName;
    private final String topicName;
    private final String jobName;
    private final int taskId;
    private final int partitionCount;

    public Context(
        VeniceProperties taskProperties,
        String storeName,
        String topicName,
        String jobName,
        int taskId,
        int partitionCount) {
      this.taskProperties = Objects.requireNonNull(taskProperties, "taskProperties");
      this.storeName = Objects.requireNonNull(storeName, "storeName");
      this.topicName = Objects.requireNonNull(topicName, "topicName");
      this.jobName = Objects.requireNonNull(jobName, "jobName");
      this.taskId = taskId;
      this.partitionCount = partitionCount;
    }

    public VeniceProperties getTaskProperties() {
      return taskProperties;
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
     * The compute-engine task ID, which is the destination partition ID for a VPJ partition writer.
     */
    public int getTaskId() {
      return taskId;
    }

    public int getPartitionCount() {
      return partitionCount;
    }
  }
}
