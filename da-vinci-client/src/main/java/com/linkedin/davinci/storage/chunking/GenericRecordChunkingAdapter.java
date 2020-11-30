package com.linkedin.davinci.storage.chunking;

import org.apache.avro.generic.GenericRecord;


/**
 * Just for the sake of casting the generic type to {@link GenericRecord}...
 */
public class GenericRecordChunkingAdapter extends GenericChunkingAdapter<GenericRecord> {
  public static final GenericRecordChunkingAdapter INSTANCE = new GenericRecordChunkingAdapter();

  /** Singleton */
  protected GenericRecordChunkingAdapter() {
    super();
  }
}
