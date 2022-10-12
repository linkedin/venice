package com.linkedin.venice.hadoop.schema;

import com.linkedin.venice.schema.rmd.RmdVersionId;
import java.io.IOException;
import java.util.Map;
import org.apache.avro.Schema;


public interface RmdSchemaSource {
  Map<RmdVersionId, Schema> fetchSchemas() throws IOException;
}
