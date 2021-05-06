package com.linkedin.davinci.storage.chunking;

import com.linkedin.venice.compression.CompressorFactory;
import org.apache.avro.generic.GenericRecord;


/**
 * Just for the sake of casting the generic type to {@link GenericRecord}...
 */
public class GenericRecordChunkingAdapter extends GenericChunkingAdapter<GenericRecord> {
  public GenericRecordChunkingAdapter(CompressorFactory compressorFactory) {
    super(compressorFactory);
  }
}
