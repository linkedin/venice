package com.linkedin.davinci.replication.merge;

import com.linkedin.davinci.schema.merge.CollectionTimestampMergeRecordHelper;
import com.linkedin.davinci.schema.merge.MergeRecordHelper;
import com.linkedin.davinci.schema.writecompute.WriteComputeProcessor;
import org.apache.avro.generic.GenericData;


public class MergeConflictResolverFactory {
  private static final MergeConflictResolverFactory INSTANCE = new MergeConflictResolverFactory();

  public static MergeConflictResolverFactory getInstance() {
    return INSTANCE;
  }

  private MergeConflictResolverFactory() {
    // Singleton class
  }

  public MergeConflictResolver createMergeConflictResolver(
      StringAnnotatedStoreSchemaCache annotatedReadOnlySchemaRepository,
      RmdSerDe rmdSerDe,
      String storeName,
      boolean rmdUseFieldLevelTs,
      boolean fastAvroEnabled) {
    MergeRecordHelper mergeRecordHelper = new CollectionTimestampMergeRecordHelper();
    return new MergeConflictResolver(
        annotatedReadOnlySchemaRepository,
        storeName,
        valueSchemaID -> new GenericData.Record(rmdSerDe.getRmdSchema(valueSchemaID)),
        new MergeGenericRecord(new WriteComputeProcessor(mergeRecordHelper), mergeRecordHelper),
        new MergeByteBuffer(),
        new MergeResultValueSchemaResolverImpl(annotatedReadOnlySchemaRepository, storeName),
        rmdSerDe,
        rmdUseFieldLevelTs,
        fastAvroEnabled);
  }

  public MergeConflictResolver createMergeConflictResolver(
      StringAnnotatedStoreSchemaCache annotatedReadOnlySchemaRepository,
      RmdSerDe rmdSerDe,
      String storeName) {
    return createMergeConflictResolver(annotatedReadOnlySchemaRepository, rmdSerDe, storeName, false, true);
  }
}
