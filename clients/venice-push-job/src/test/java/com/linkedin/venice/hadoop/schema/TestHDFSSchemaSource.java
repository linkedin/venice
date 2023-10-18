package com.linkedin.venice.hadoop.schema;

import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
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

  private final static String TEST_SCHEMA =
      "{\"type\":\"record\"," + "\"name\":\"User\"," + "\"namespace\":\"example.avro\"," + "\"fields\":["
          + "{\"name\":\"name\",\"type\":\"string\",\"default\":\"venice\"}]}";
  private HDFSSchemaSource source;
  private ControllerClient client;

  @BeforeClass
  public void setUp() throws IOException {
    File rmdInputDir = getTempDataDirectory();
    File valueInputDir = getTempDataDirectory();
    client = mock(ControllerClient.class);
    MultiSchemaResponse.Schema[] schemas = generateMultiSchema(numOfSchemas);
    MultiSchemaResponse response = new MultiSchemaResponse();
    response.setSchemas(schemas);
    doReturn(response).when(client).getAllReplicationMetadataSchemas(TEST_STORE);
    doReturn(response).when(client).getAllValueSchema(TEST_STORE);

    source = new HDFSSchemaSource(valueInputDir.getAbsolutePath(), rmdInputDir.getAbsolutePath(), TEST_STORE);
  }

  @Test
  public void testLoadRmdSchemaThenFetch() throws IOException {
    source.saveSchemasOnDisk(client);
    Map<RmdVersionId, Schema> schemaMap = source.fetchRmdSchemas();
    Assert.assertEquals(numOfSchemas, schemaMap.size());
    for (int i = 1; i <= numOfSchemas; i++) {
      Schema schema = schemaMap.get(new RmdVersionId(i, i));
      Assert.assertEquals(schema.toString(), TEST_SCHEMA);
    }
  }

  private MultiSchemaResponse.Schema[] generateMultiSchema(int n) {
    MultiSchemaResponse.Schema[] response = new MultiSchemaResponse.Schema[n];
    for (int i = 1; i <= n; i++) {
      MultiSchemaResponse.Schema schema = new MultiSchemaResponse.Schema();
      schema.setRmdSchemaId(i);
      schema.setDerivedSchemaId(i);
      schema.setId(i);
      schema.setSchemaStr(TEST_SCHEMA);
      response[i - 1] = schema;
    }
    return response;
  }
}
