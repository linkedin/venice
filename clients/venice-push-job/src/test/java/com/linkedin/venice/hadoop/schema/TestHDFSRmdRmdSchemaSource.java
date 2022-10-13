package com.linkedin.venice.hadoop.schema;

import static com.linkedin.venice.hadoop.VenicePushJob.VENICE_STORE_NAME_PROP;
import static com.linkedin.venice.utils.TestPushUtils.getTempDataDirectory;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.schema.rmd.RmdVersionId;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestHDFSRmdRmdSchemaSource {
  private final static String TEST_STORE = "test_store";
  private final static int numOfSchemas = 3;

  private final static String TEST_SCHEMA =
      "{\"type\":\"record\"," + "\"name\":\"User\"," + "\"namespace\":\"example.avro\"," + "\"fields\":["
          + "{\"name\":\"name\",\"type\":\"string\",\"default\":\"venice\"}]}";
  private HDFSRmdRmdSchemaSource source;

  @BeforeClass
  public void setUp() throws IOException {
    Properties props = new Properties();
    props.setProperty(VENICE_STORE_NAME_PROP, TEST_STORE);
    File inputDir = getTempDataDirectory();

    ControllerClient client = mock(ControllerClient.class);
    MultiSchemaResponse.Schema[] schemas = generateMultiSchema(numOfSchemas);
    MultiSchemaResponse response = new MultiSchemaResponse();
    response.setSchemas(schemas);
    doReturn(response).when(client).getAllReplicationMetadataSchemas(TEST_STORE);

    source = new HDFSRmdRmdSchemaSource(inputDir.getAbsolutePath(), new VeniceProperties(props), client);
  }

  @Test
  public void testLoadRmdSchemaThenFetch() throws IOException {
    source.loadRmdSchemasOnDisk();
    Map<RmdVersionId, Schema> schemaMap = source.fetchSchemas();
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
      schema.setRmdValueSchemaId(i);
      schema.setDerivedSchemaId(i);
      schema.setId(i);
      schema.setSchemaStr(TEST_SCHEMA);
      response[i - 1] = schema;
    }
    return response;
  }
}
