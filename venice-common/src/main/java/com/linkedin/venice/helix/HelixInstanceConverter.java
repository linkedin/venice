package com.linkedin.venice.helix;

import com.linkedin.venice.meta.Instance;
import org.apache.helix.ZNRecord;


/**
 * Convert between ZNRecord in Helix instanceConfig and Venice instance.
 */
public class HelixInstanceConverter {
    private static final String HOST = "HOST";
    private static final String HTTP_PORT = "HTTP_PORT";
    private static final String ADMIN_PORT = "ADMIN_PORT";

    public static ZNRecord convertInstanceToZNRecord(Instance instance) {
        ZNRecord record = new ZNRecord(instance.getNodeId());
        record.setSimpleField(HOST, instance.getHost());
        record.setIntField(HTTP_PORT, instance.getHttpPort());
        record.setIntField(ADMIN_PORT, instance.getAdminPort());
        return record;
    }

    public static Instance convertZNRecordToInstance(ZNRecord record) {
        return new Instance(record.getId(), record.getSimpleField(HOST), record.getIntField(ADMIN_PORT, -1),
            record.getIntField(HTTP_PORT, -1));
    }
}
