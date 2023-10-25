package com.linkedin.venice.hadoop.schema;

import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.davinci.schema.SchemaUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.schema.rmd.RmdVersionId;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestHDFSSchemaSource {
  private final static String TEST_STORE = "test_store";
  private final static int numOfSchemas = 3;

  private final static String VALUE_SCHEMA_STRING =
      "{\"type\":\"record\"," + "\"name\":\"User\"," + "\"namespace\":\"example.avro\"," + "\"fields\":["
          + "{\"name\":\"name\",\"type\":\"string\",\"default\":\"venice\"}]}";
  private final static Schema VALUE_SCHEMA = AvroSchemaParseUtils.parseSchemaFromJSON(VALUE_SCHEMA_STRING, false);

  private final static Schema RMD_SCHEMA = RmdSchemaGenerator.generateMetadataSchema(VALUE_SCHEMA);
  private final static Schema ANNOTATED_VALUE_SCHEMA = SchemaUtils.annotateValueSchema(VALUE_SCHEMA);
  private final static Schema ANNOTATED_RMD_SCHEMA = SchemaUtils.annotateRmdSchema(RMD_SCHEMA);

  private HDFSSchemaSource source;
  private ControllerClient client;

  @BeforeClass
  public void setUp() throws IOException {
    File rmdInputDir = getTempDataDirectory();
    File valueInputDir = getTempDataDirectory();
    client = mock(ControllerClient.class);
    MultiSchemaResponse.Schema[] rmdSchemas = generateRmdSchemas(numOfSchemas);
    MultiSchemaResponse rmdSchemaResponse = new MultiSchemaResponse();
    rmdSchemaResponse.setSchemas(rmdSchemas);

    MultiSchemaResponse.Schema[] valueSchemas = generateValueSchema(numOfSchemas);
    MultiSchemaResponse valueSchemaResponse = new MultiSchemaResponse();
    valueSchemaResponse.setSchemas(valueSchemas);
    doReturn(rmdSchemaResponse).when(client).getAllReplicationMetadataSchemas(TEST_STORE);
    doReturn(valueSchemaResponse).when(client).getAllValueSchema(TEST_STORE);

    source = new HDFSSchemaSource(valueInputDir.getAbsolutePath(), rmdInputDir.getAbsolutePath(), TEST_STORE);
    source.saveSchemasOnDisk(client);

  }

  @Test
  public void testLoadRmdSchemaThenFetch() throws IOException {
    Map<RmdVersionId, Schema> schemaMap = source.fetchRmdSchemas();
    Assert.assertEquals(numOfSchemas, schemaMap.size());
    for (int i = 1; i <= numOfSchemas; i++) {
      Schema schema = schemaMap.get(new RmdVersionId(i, i));
      Assert.assertEquals(schema.toString(), ANNOTATED_RMD_SCHEMA.toString());
    }
  }

  @Test
  public void testLoadValueSchemaThenFetch() throws IOException {
    Map<Integer, Schema> schemaMap = source.fetchValueSchemas();
    Assert.assertEquals(numOfSchemas, schemaMap.size());
    for (int i = 1; i <= numOfSchemas; i++) {
      Schema schema = schemaMap.get(i);
      Assert.assertEquals(schema.toString(), ANNOTATED_VALUE_SCHEMA.toString());
    }
  }

  private MultiSchemaResponse.Schema[] generateRmdSchemas(int n) {
    MultiSchemaResponse.Schema[] response = new MultiSchemaResponse.Schema[n];
    for (int i = 1; i <= n; i++) {
      MultiSchemaResponse.Schema schema = new MultiSchemaResponse.Schema();
      schema.setRmdSchemaId(i);
      schema.setDerivedSchemaId(i);
      schema.setId(i);
      schema.setSchemaStr(RMD_SCHEMA.toString());
      response[i - 1] = schema;
    }
    return response;
  }

  private MultiSchemaResponse.Schema[] generateValueSchema(int n) {
    MultiSchemaResponse.Schema[] response = new MultiSchemaResponse.Schema[n];
    for (int i = 1; i <= n; i++) {
      MultiSchemaResponse.Schema schema = new MultiSchemaResponse.Schema();
      schema.setId(i);
      schema.setSchemaStr(VALUE_SCHEMA_STRING);
      response[i - 1] = schema;
    }
    return response;
  }
}
