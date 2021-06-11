package com.linkedin.venice.pushstatus;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import org.apache.avro.specific.SpecificData;


@SuppressWarnings("all")
public class PushStatusValueWriteOpRecord extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = AvroCompatibilityHelper.parse("{\"type\":\"record\",\"name\":\"PushStatusValueWriteOpRecord\",\"namespace\":\"com.linkedin.venice.pushstatus\",\"fields\":[{\"name\":\"instances\",\"type\":[{\"type\":\"record\",\"name\":\"NoOp\",\"fields\":[]},{\"type\":\"record\",\"name\":\"instancesMapOps\",\"fields\":[{\"name\":\"mapUnion\",\"type\":{\"type\":\"map\",\"values\":\"int\"},\"default\":{}},{\"name\":\"mapDiff\",\"type\":{\"type\":\"array\",\"items\":\"string\"},\"default\":[]}]},{\"type\":\"map\",\"values\":\"int\"}],\"default\":{}},{\"name\":\"reportTimestamp\",\"type\":[\"NoOp\",\"null\",\"long\"],\"doc\":\"heartbeat.\",\"default\":{}}]}");
 private static org.apache.avro.specific.SpecificData MODEL$ = SpecificData.get();
 
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  public java.lang.Object instances;
  /** heartbeat. */
  public java.lang.Object reportTimestamp;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return instances;
    case 1: return reportTimestamp;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: instances = (java.lang.Object)value$; break;
    case 1: reportTimestamp = (java.lang.Object)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
