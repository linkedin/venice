package com.linkedin.venice.pushstatus;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import org.apache.avro.specific.SpecificData;


@SuppressWarnings("all")
public class instancesMapOps extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = AvroCompatibilityHelper.parse("{\"type\":\"record\",\"name\":\"instancesMapOps\",\"namespace\":\"com.linkedin.venice.pushstatus\",\"fields\":[{\"name\":\"mapUnion\",\"type\":{\"type\":\"map\",\"values\":\"int\"},\"default\":{}},{\"name\":\"mapDiff\",\"type\":{\"type\":\"array\",\"items\":\"string\"},\"default\":[]}]}");
 private static org.apache.avro.specific.SpecificData MODEL$ = SpecificData.get();
 
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  public java.util.Map<java.lang.CharSequence,java.lang.Integer> mapUnion;
  public java.util.List<java.lang.CharSequence> mapDiff;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return mapUnion;
    case 1: return mapDiff;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: mapUnion = (java.util.Map<java.lang.CharSequence,java.lang.Integer>)value$; break;
    case 1: mapDiff = (java.util.List<java.lang.CharSequence>)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
