package com.linkedin.venice.hadoop;

import com.linkedin.venice.schema.vson.VsonSchema;
import com.linkedin.venice.utils.Pair;
import org.apache.avro.Schema;


public class PushJobSchemaInfo {
  private boolean isAvro = true;
  private int valueSchemaId; // Value schema id retrieved from backend for valueSchemaString
  private int derivedSchemaId = -1;
  private String keyField;
  private String valueField;
  private String fileSchemaString;
  private String keySchemaString;
  private String valueSchemaString;
  private String vsonFileKeySchema;
  private String vsonFileValueSchema;
  private Pair<Schema, Schema> avroSchema;
  private Pair<VsonSchema, VsonSchema> vsonSchema;

  public boolean isAvro() {
    return isAvro;
  }

  public void setAvro(boolean avro) {
    isAvro = avro;
  }

  public int getValueSchemaId() {
    return valueSchemaId;
  }

  public void setValueSchemaId(int valueSchemaId) {
    this.valueSchemaId = valueSchemaId;
  }

  public int getDerivedSchemaId() {
    return derivedSchemaId;
  }

  public void setDerivedSchemaId(int derivedSchemaId) {
    this.derivedSchemaId = derivedSchemaId;
  }

  public String getKeyField() {
    return keyField;
  }

  public void setKeyField(String keyField) {
    this.keyField = keyField;
  }

  public String getValueField() {
    return valueField;
  }

  public void setValueField(String valueField) {
    this.valueField = valueField;
  }

  public String getFileSchemaString() {
    return fileSchemaString;
  }

  public void setFileSchemaString(String fileSchemaString) {
    this.fileSchemaString = fileSchemaString;
  }

  public String getKeySchemaString() {
    return keySchemaString;
  }

  public void setKeySchemaString(String keySchemaString) {
    this.keySchemaString = keySchemaString;
  }

  public String getValueSchemaString() {
    return valueSchemaString;
  }

  public void setValueSchemaString(String valueSchemaString) {
    this.valueSchemaString = valueSchemaString;
  }

  public String getVsonFileKeySchema() {
    return vsonFileKeySchema;
  }

  public void setVsonFileKeySchema(String vsonFileKeySchema) {
    this.vsonFileKeySchema = vsonFileKeySchema;
  }

  public String getVsonFileValueSchema() {
    return vsonFileValueSchema;
  }

  public void setVsonFileValueSchema(String vsonFileValueSchema) {
    this.vsonFileValueSchema = vsonFileValueSchema;
  }

  public Pair<Schema, Schema> getAvroSchema() {
    return avroSchema;
  }

  public void setAvroSchema(Pair<Schema, Schema> avroSchema) {
    this.avroSchema = avroSchema;
  }

  public Pair<VsonSchema, VsonSchema> getVsonSchema() {
    return vsonSchema;
  }

  public void setVsonSchema(Pair<VsonSchema, VsonSchema> vsonSchema) {
    this.vsonSchema = vsonSchema;
  }
}
