package com.linkedin.venice.schema.rmd.v2;

import com.linkedin.venice.schema.rmd.v1.ReplicationMetadataSchemaGeneratorV1;
import org.apache.avro.Schema;


/**
 * Replication metadata schema adapter with nested collection support. Create finer metadata schema
 * when collections are encountered. Right now, we have 3 layers TS metadata.
 * 1. TS for the whole record.
 * 2. TS for each field.
 * 3. TS for each elements in the collections.
 *
 * {@link CollectionReplicationMetadata#createCollectionTimeStampSchema} for more implementation details
 */

public class ReplicationMetadataSchemaGeneratorV2 extends ReplicationMetadataSchemaGeneratorV1 {

  public ReplicationMetadataSchemaGeneratorV2() {}

  @Override
  public Schema generateMetadataSchemaFromRecord(Schema recordSchema, String namespace) {
    final RecordMetadataSchemaBuilder recordMetadataSchemaBuilder = new RecordMetadataSchemaBuilder();
    recordMetadataSchemaBuilder.setValueRecordSchema(recordSchema);
    recordMetadataSchemaBuilder.setNamespace(namespace);
    return recordMetadataSchemaBuilder.build();
  }
}
