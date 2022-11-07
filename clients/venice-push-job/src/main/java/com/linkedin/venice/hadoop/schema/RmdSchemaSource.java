package com.linkedin.venice.hadoop.schema;

import com.linkedin.venice.schema.rmd.RmdVersionId;
import java.io.IOException;
import java.util.Map;
import org.apache.avro.Schema;


/**
 * A high-level interface as a source specifically for RmdSchema. Each RMD Schema is uniquely identified by {@link RmdVersionId}.
 */
public interface RmdSchemaSource {
  Map<RmdVersionId, Schema> fetchSchemas() throws IOException;
}
