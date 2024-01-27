package com.linkedin.venice;

import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;


public class ValueAndDerivedSchemaData {
  private final Schema valueSchema;
  private final Map<Integer, Schema> updateSchemaMap = new VeniceConcurrentHashMap<>();
  private final Map<Integer, Schema> rmdSchemaMap = new VeniceConcurrentHashMap<>();
  private final GenericDatumReader<Object> valueRecordReader;
  private final Map<Integer, GenericDatumReader<Object>> updateRecordReaderMap = new VeniceConcurrentHashMap<>();
  private final Map<Integer, GenericDatumReader<Object>> rmdRecordReaderMap = new VeniceConcurrentHashMap<>();

  public ValueAndDerivedSchemaData(String valueSchemaStr) {
    this.valueSchema = AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation(valueSchemaStr);
    this.valueRecordReader = new GenericDatumReader<>(valueSchema, valueSchema);
  }

  public void setUpdateSchema(int protocolId, String updateSchemaStr) {
    Schema schema = AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation(updateSchemaStr);
    updateSchemaMap.put(protocolId, schema);
    updateRecordReaderMap.put(protocolId, new GenericDatumReader<>(schema, schema));
  }

  public void setRmdSchema(int protocolId, String rmdSchemaStr) {
    Schema schema = AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation(rmdSchemaStr);
    rmdSchemaMap.put(protocolId, schema);
    rmdRecordReaderMap.put(protocolId, new GenericDatumReader<>(schema, schema));
  }

  GenericDatumReader<Object> getValueRecordReader() {
    return valueRecordReader;
  }

  GenericDatumReader<Object> getUpdateRecordReader(int protocolId) {
    return updateRecordReaderMap.get(protocolId);
  }

  GenericDatumReader<Object> getRmdRecordReader(int protocolId) {
    return rmdRecordReaderMap.get(protocolId);
  }

}
