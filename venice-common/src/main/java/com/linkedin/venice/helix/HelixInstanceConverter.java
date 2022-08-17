package com.linkedin.venice.helix;

import com.linkedin.venice.meta.Instance;
import org.apache.helix.zookeeper.datamodel.ZNRecord;


/**
 * Convert between ZNRecord in Helix instanceConfig and Venice instance.
 */
public class HelixInstanceConverter {
  private static final String HOST = "HOST";
  private static final String PORT = "PORT";

  public static ZNRecord convertInstanceToZNRecord(Instance instance) {
    ZNRecord record = new ZNRecord(instance.getNodeId());
    record.setSimpleField(HOST, instance.getHost());
    record.setIntField(PORT, instance.getPort());
    return record;
  }

  public static Instance convertZNRecordToInstance(ZNRecord record) {
    return new Instance(record.getId(), record.getSimpleField(HOST), record.getIntField(PORT, -1));
  }
}
