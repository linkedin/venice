package com.linkedin.davinci.storage.chunking;

import org.apache.avro.generic.GenericRecord;


/**
 * Read compute and write compute chunking adapter
 */
public class GenericChunkingAdapter<V extends GenericRecord> extends AbstractAvroChunkingAdapter<V> {
  public static final GenericChunkingAdapter INSTANCE = new GenericChunkingAdapter();

  /** Singleton */
  protected GenericChunkingAdapter() {
  }
}
