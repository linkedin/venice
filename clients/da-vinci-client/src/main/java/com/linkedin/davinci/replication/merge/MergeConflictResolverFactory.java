package com.linkedin.davinci.replication.merge;

import com.linkedin.venice.schema.merge.CollectionTimestampMergeRecordHelper;
import com.linkedin.venice.schema.merge.MergeRecordHelper;
import com.linkedin.venice.schema.writecompute.WriteComputeProcessor;
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
      boolean rmdUseFieldLevelTs) {
    MergeRecordHelper mergeRecordHelper = new CollectionTimestampMergeRecordHelper();
    return new MergeConflictResolver(
        annotatedReadOnlySchemaRepository,
        storeName,
        valueSchemaID -> new GenericData.Record(rmdSerDe.getRmdSchema(valueSchemaID)),
        new MergeGenericRecord(new WriteComputeProcessor(mergeRecordHelper), mergeRecordHelper),
        new MergeByteBuffer(),
        new MergeResultValueSchemaResolverImpl(annotatedReadOnlySchemaRepository, storeName),
        rmdSerDe,
        rmdUseFieldLevelTs);
  }

  public MergeConflictResolver createMergeConflictResolver(
      StringAnnotatedStoreSchemaCache annotatedReadOnlySchemaRepository,
      RmdSerDe rmdSerDe,
      String storeName) {
    return createMergeConflictResolver(annotatedReadOnlySchemaRepository, rmdSerDe, storeName, false);
  }
}
