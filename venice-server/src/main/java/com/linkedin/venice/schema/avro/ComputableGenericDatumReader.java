package com.linkedin.venice.schema.avro;

import java.io.IOException;
import java.util.Collection;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.specific.SpecificDatumReader;


public class ComputableGenericDatumReader<T> extends GenericDatumReader<T> {
  public ComputableGenericDatumReader(Schema writer, Schema reader) {
    super(writer, reader);
  }

  /**
   * Overridden to be able to inject a {@link ComputablePrimitiveFloatList} if appropriate. Otherwise behaves exactly as
   * {@link org.apache.avro.generic.GenericDatumReader#readArray(Object, Schema, ResolvingDecoder)}.
   */
  @Override
  protected Object readArray(Object old, Schema expected, ResolvingDecoder in) throws IOException {
    if (ComputablePrimitiveFloatList.isFloatArray(expected)) {
      return ComputablePrimitiveFloatList.readPrimitiveFloatArray(old, in);
    }

    // Regular logic
    return super.readArray(old, expected, in);
  }
}
