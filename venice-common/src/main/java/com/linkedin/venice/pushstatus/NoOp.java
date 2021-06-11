package com.linkedin.venice.pushstatus;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import org.apache.avro.specific.SpecificData;


@SuppressWarnings("all")
public class NoOp extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = AvroCompatibilityHelper.parse("{\"type\":\"record\",\"name\":\"NoOp\",\"namespace\":\"com.linkedin.venice.pushstatus\",\"fields\":[]}");
 private static org.apache.avro.specific.SpecificData MODEL$ = SpecificData.get();
 
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
