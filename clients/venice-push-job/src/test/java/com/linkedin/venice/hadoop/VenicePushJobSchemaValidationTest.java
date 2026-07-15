package com.linkedin.venice.hadoop;

import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V1_OH_SUPERSET_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V1_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V1_UPDATE_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V2_SCHEMA;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_RMD_FIELD_PROP;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.exceptions.VeniceSchemaMismatchException;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import java.util.Properties;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * This class contains only unit tests for VenicePushJob class.
 *
 * For integration tests please refer to TestVenicePushJob
 *
 * todo: Remove dependency on utils from 'venice-test-common' module
 */

public class VenicePushJobSchemaValidationTest extends VenicePushJobTestBase {
  @Test
  public void testVPJCheckInputUpdateSchema() {
    VenicePushJob vpj = mock(VenicePushJob.class);
    when(vpj.isUpdateSchema(anyString())).thenCallRealMethod();
    Assert.assertTrue(vpj.isUpdateSchema(NAME_RECORD_V1_UPDATE_SCHEMA.toString()));
    Assert.assertFalse(vpj.isUpdateSchema(NAME_RECORD_V1_SCHEMA.toString()));
  }

  @Test(expectedExceptions = VeniceSchemaMismatchException.class)
  public void testValidateKeySchemaMismatch() {
    String keySchema = "\"string\"";
    String serverKeySchema = "\"int\"";
    VenicePushJob vpj = getSpyVenicePushJob(new Properties(), null);
    PushJobSetting setting = vpj.getPushJobSetting();
    setting.storeKeySchema = AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation(serverKeySchema);
    setting.keySchema = AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation(keySchema);
    vpj.validateKeySchema(vpj.getPushJobSetting());
  }

  @Test
  public void testValidateAndRetrieveRmdSchemaWithNoRmdField() {
    PushJobSetting setting = new PushJobSetting();
    setting.storeName = TEST_STORE;
    ControllerClient mockClient = mock(ControllerClient.class);
    VenicePushJob vpj = mock(VenicePushJob.class);
    doCallRealMethod().when(vpj).validateAndSetRmdSchemas(any(), any());
    vpj.validateAndSetRmdSchemas(mockClient, setting);
    verifyNoInteractions(mockClient);
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testValidateAndRetrieveRmdSchemaWithErrorResponseFromController() {
    PushJobSetting setting = new PushJobSetting();
    setting.rmdField = DEFAULT_RMD_FIELD_PROP;
    setting.storeName = TEST_STORE;
    setting.controllerRetries = 1;
    ControllerClient mockClient = mock(ControllerClient.class);

    MultiSchemaResponse mockResponse = mock(MultiSchemaResponse.class);
    when(mockResponse.isError()).thenReturn(true);
    when(mockClient.getAllReplicationMetadataSchemas(eq(TEST_STORE))).thenReturn(mockResponse);
    VenicePushJob vpj = mock(VenicePushJob.class);
    doCallRealMethod().when(vpj).validateAndSetRmdSchemas(any(), any());
    vpj.validateAndSetRmdSchemas(mockClient, setting);
  }

  @Test
  public void testValidateAndRetrieveRmdSchema() {
    PushJobSetting setting = new PushJobSetting();
    setting.rmdField = DEFAULT_RMD_FIELD_PROP;
    setting.storeName = TEST_STORE;
    setting.controllerRetries = 1;
    setting.valueSchemaId = 1;
    ControllerClient mockClient = mock(ControllerClient.class);

    MultiSchemaResponse mockResponse = mock(MultiSchemaResponse.class);
    MultiSchemaResponse.Schema rmdSchema = mock(MultiSchemaResponse.Schema.class);
    when(rmdSchema.getRmdValueSchemaId()).thenReturn(1);
    when(rmdSchema.getId()).thenReturn(1);
    when(rmdSchema.getSchemaStr()).thenReturn(RMD_SCHEMA_STR);
    when(mockResponse.getSchemas()).thenReturn(new MultiSchemaResponse.Schema[] { rmdSchema });
    when(mockResponse.isError()).thenReturn(false);
    when(mockClient.getAllReplicationMetadataSchemas(eq(TEST_STORE))).thenReturn(mockResponse);
    VenicePushJob vpj = mock(VenicePushJob.class);
    doCallRealMethod().when(vpj).validateAndSetRmdSchemas(any(), any());
    vpj.validateAndSetRmdSchemas(mockClient, setting);
    Assert.assertEquals(setting.replicationMetadataSchemaString, RMD_SCHEMA_STR);
    Assert.assertEquals(setting.rmdSchemaId, 1);
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Cannot continue with push with RMD since the RMD schema for the value.*")
  public void testValidateAndRetrieveRmdSchemaWithMultipleRmdSchemas() {
    PushJobSetting setting = new PushJobSetting();
    setting.rmdField = DEFAULT_RMD_FIELD_PROP;
    setting.storeName = TEST_STORE;
    setting.controllerRetries = 1;
    setting.valueSchemaId = 1;
    ControllerClient mockClient = mock(ControllerClient.class);

    MultiSchemaResponse mockResponse = mock(MultiSchemaResponse.class);
    MultiSchemaResponse.Schema rmdSchemaV1 = mock(MultiSchemaResponse.Schema.class);
    when(rmdSchemaV1.getRmdValueSchemaId()).thenReturn(1);
    when(rmdSchemaV1.getId()).thenReturn(1);
    when(rmdSchemaV1.getSchemaStr()).thenReturn(RMD_SCHEMA_STR);

    MultiSchemaResponse.Schema rmdSchemaV2 = mock(MultiSchemaResponse.Schema.class);
    when(rmdSchemaV2.getRmdValueSchemaId()).thenReturn(1);
    when(rmdSchemaV2.getId()).thenReturn(2);
    when(rmdSchemaV2.getSchemaStr()).thenReturn(RMD_SCHEMA_STR_V2);

    when(mockResponse.getSchemas()).thenReturn(new MultiSchemaResponse.Schema[] { rmdSchemaV1, rmdSchemaV2 });
    when(mockResponse.isError()).thenReturn(false);
    when(mockClient.getAllReplicationMetadataSchemas(eq(TEST_STORE))).thenReturn(mockResponse);
    VenicePushJob vpj = mock(VenicePushJob.class);
    doCallRealMethod().when(vpj).validateAndSetRmdSchemas(any(), any());
    vpj.validateAndSetRmdSchemas(mockClient, setting);
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testValidateAndRetrieveRmdSchemaWithNoRmdSchemaForValueSchema() {
    PushJobSetting setting = new PushJobSetting();
    setting.rmdField = DEFAULT_RMD_FIELD_PROP;
    setting.storeName = TEST_STORE;
    setting.controllerRetries = 1;
    setting.valueSchemaId = 1;
    ControllerClient mockClient = mock(ControllerClient.class);

    MultiSchemaResponse mockResponse = mock(MultiSchemaResponse.class);
    MultiSchemaResponse.Schema rmdSchema = mock(MultiSchemaResponse.Schema.class);
    when(rmdSchema.getRmdValueSchemaId()).thenReturn(2);
    when(rmdSchema.getId()).thenReturn(1);
    when(rmdSchema.getSchemaStr()).thenReturn(RMD_SCHEMA_STR);
    when(mockResponse.getSchemas()).thenReturn(new MultiSchemaResponse.Schema[] { rmdSchema });
    when(mockResponse.isError()).thenReturn(false);
    when(mockClient.getAllReplicationMetadataSchemas(eq(TEST_STORE))).thenReturn(mockResponse);
    VenicePushJob vpj = mock(VenicePushJob.class);
    doCallRealMethod().when(vpj).validateAndSetRmdSchemas(any(), any());
    vpj.validateAndSetRmdSchemas(mockClient, setting);
  }

  @Test
  public void testValidateValueSchemaEnablesProjectionForSupersetInput() {
    // Input value schema (V2) is a strict superset of the chosen target writer value schema (V1).
    ControllerClient mockClient = mock(ControllerClient.class);

    // Input value schema does not match any registered value schema.
    SchemaResponse valueSchemaIdResponse = new SchemaResponse();
    valueSchemaIdResponse.setError("Could not find any registered value schema for the input value schema.");
    doReturn(valueSchemaIdResponse).when(mockClient)
        .getValueSchemaID(eq(TEST_STORE), eq(NAME_RECORD_V2_SCHEMA.toString()));

    // The supplied target writer value schema id resolves to the writer (V1) schema.
    SchemaResponse writerSchemaResponse = new SchemaResponse();
    writerSchemaResponse.setId(7);
    writerSchemaResponse.setSchemaStr(NAME_RECORD_V1_SCHEMA.toString());
    doReturn(writerSchemaResponse).when(mockClient).getValueSchema(eq(TEST_STORE), eq(7));

    VenicePushJob vpj = getSpyVenicePushJob(new Properties(), mockClient);
    PushJobSetting setting = vpj.getPushJobSetting();
    setting.storeName = TEST_STORE;
    setting.controllerRetries = 1;
    setting.enableWriteCompute = false;
    setting.targetWriterValueSchemaId = 7;
    setting.valueSchema = NAME_RECORD_V2_SCHEMA;
    setting.valueSchemaString = NAME_RECORD_V2_SCHEMA.toString();

    vpj.validateAndRetrieveValueSchemas(mockClient, setting, false);

    Assert.assertTrue(setting.projectInputToWriterSchema);
    Assert.assertEquals(setting.valueSchemaId, 7);
    Assert.assertEquals(setting.writerValueSchemaString, NAME_RECORD_V1_SCHEMA.toString());
    Assert.assertEquals(setting.writerValueSchema, NAME_RECORD_V1_SCHEMA);
  }

  @Test
  public void testValidateValueSchemaEnablesProjectionForNullableDriftSupersetInput() {
    // OH schema evolution wraps the writer's non-nullable fields as [null, X] and appends extra nullable fields, so
    // the input value schema is a projection-superset (but NOT a strict subset) of the V1 writer schema. The
    // projection eligibility check must accept this nullability drift and enable projection.
    ControllerClient mockClient = mock(ControllerClient.class);

    Schema ohSupersetInput = NAME_RECORD_V1_OH_SUPERSET_SCHEMA;

    // Input value schema does not match any registered value schema.
    SchemaResponse valueSchemaIdResponse = new SchemaResponse();
    valueSchemaIdResponse.setError("Could not find any registered value schema for the input value schema.");
    doReturn(valueSchemaIdResponse).when(mockClient).getValueSchemaID(eq(TEST_STORE), eq(ohSupersetInput.toString()));

    // The supplied target writer value schema id resolves to the non-nullable V1 writer schema.
    SchemaResponse writerSchemaResponse = new SchemaResponse();
    writerSchemaResponse.setId(7);
    writerSchemaResponse.setSchemaStr(NAME_RECORD_V1_SCHEMA.toString());
    doReturn(writerSchemaResponse).when(mockClient).getValueSchema(eq(TEST_STORE), eq(7));

    VenicePushJob vpj = getSpyVenicePushJob(new Properties(), mockClient);
    PushJobSetting setting = vpj.getPushJobSetting();
    setting.storeName = TEST_STORE;
    setting.controllerRetries = 1;
    setting.enableWriteCompute = false;
    setting.targetWriterValueSchemaId = 7;
    setting.valueSchema = ohSupersetInput;
    setting.valueSchemaString = ohSupersetInput.toString();

    vpj.validateAndRetrieveValueSchemas(mockClient, setting, false);

    Assert.assertTrue(setting.projectInputToWriterSchema);
    Assert.assertEquals(setting.valueSchemaId, 7);
    Assert.assertEquals(setting.writerValueSchemaString, NAME_RECORD_V1_SCHEMA.toString());
    Assert.assertEquals(setting.writerValueSchema, NAME_RECORD_V1_SCHEMA);
  }

  @Test(expectedExceptions = VeniceSchemaMismatchException.class, expectedExceptionsMessageRegExp = ".*not a superset of the target writer value schema.*")
  public void testValidateValueSchemaRejectsReverseNullableDriftProjectionInput() {
    // The relaxed projection check must not become a blanket "accept everything". Here the input is the non-nullable V1
    // schema while the target writer is the OH-superset (its fields are [null, X] and it has an extra "age"). That is
    // reverse nullability drift plus a missing writer field -- the input is NOT a projection-superset of the writer --
    // so projection eligibility must be rejected.
    ControllerClient mockClient = mock(ControllerClient.class);

    SchemaResponse valueSchemaIdResponse = new SchemaResponse();
    valueSchemaIdResponse.setError("Could not find any registered value schema for the input value schema.");
    doReturn(valueSchemaIdResponse).when(mockClient)
        .getValueSchemaID(eq(TEST_STORE), eq(NAME_RECORD_V1_SCHEMA.toString()));

    SchemaResponse writerSchemaResponse = new SchemaResponse();
    writerSchemaResponse.setId(7);
    writerSchemaResponse.setSchemaStr(NAME_RECORD_V1_OH_SUPERSET_SCHEMA.toString());
    doReturn(writerSchemaResponse).when(mockClient).getValueSchema(eq(TEST_STORE), eq(7));

    VenicePushJob vpj = getSpyVenicePushJob(new Properties(), mockClient);
    PushJobSetting setting = vpj.getPushJobSetting();
    setting.storeName = TEST_STORE;
    setting.controllerRetries = 1;
    setting.enableWriteCompute = false;
    setting.targetWriterValueSchemaId = 7;
    setting.valueSchema = NAME_RECORD_V1_SCHEMA;
    setting.valueSchemaString = NAME_RECORD_V1_SCHEMA.toString();

    vpj.validateAndRetrieveValueSchemas(mockClient, setting, false);
  }

  @Test
  public void testValidateValueSchemaSkipsProjectionWhenInputMatchesDifferentRegisteredSchemaId() {
    // Input value schema already matches a registered value schema (id 3), but the supplied target writer value schema
    // id (7) is different. Projection must be skipped and the matched schema id used.
    ControllerClient mockClient = mock(ControllerClient.class);

    SchemaResponse valueSchemaIdResponse = new SchemaResponse();
    valueSchemaIdResponse.setId(3);
    valueSchemaIdResponse.setSchemaStr(NAME_RECORD_V2_SCHEMA.toString());
    doReturn(valueSchemaIdResponse).when(mockClient)
        .getValueSchemaID(eq(TEST_STORE), eq(NAME_RECORD_V2_SCHEMA.toString()));

    VenicePushJob vpj = getSpyVenicePushJob(new Properties(), mockClient);
    PushJobSetting setting = vpj.getPushJobSetting();
    setting.storeName = TEST_STORE;
    setting.controllerRetries = 1;
    setting.enableWriteCompute = false;
    setting.targetWriterValueSchemaId = 7;
    setting.valueSchema = NAME_RECORD_V2_SCHEMA;
    setting.valueSchemaString = NAME_RECORD_V2_SCHEMA.toString();

    vpj.validateAndRetrieveValueSchemas(mockClient, setting, false);

    Assert.assertFalse(setting.projectInputToWriterSchema);
    Assert.assertEquals(setting.valueSchemaId, 3);
    verify(mockClient, never()).getValueSchema(eq(TEST_STORE), eq(7));
  }

  @Test(expectedExceptions = VeniceSchemaMismatchException.class, expectedExceptionsMessageRegExp = ".*not a superset of the target writer value schema.*")
  public void testValidateValueSchemaRejectsNonSupersetProjectionInput() {
    // Input value schema (V1) is NOT a superset of the chosen target writer value schema (V2): V2 requires "age".
    ControllerClient mockClient = mock(ControllerClient.class);

    SchemaResponse valueSchemaIdResponse = new SchemaResponse();
    valueSchemaIdResponse.setError("Could not find any registered value schema for the input value schema.");
    doReturn(valueSchemaIdResponse).when(mockClient)
        .getValueSchemaID(eq(TEST_STORE), eq(NAME_RECORD_V1_SCHEMA.toString()));

    SchemaResponse writerSchemaResponse = new SchemaResponse();
    writerSchemaResponse.setId(7);
    writerSchemaResponse.setSchemaStr(NAME_RECORD_V2_SCHEMA.toString());
    doReturn(writerSchemaResponse).when(mockClient).getValueSchema(eq(TEST_STORE), eq(7));

    VenicePushJob vpj = getSpyVenicePushJob(new Properties(), mockClient);
    PushJobSetting setting = vpj.getPushJobSetting();
    setting.storeName = TEST_STORE;
    setting.controllerRetries = 1;
    setting.enableWriteCompute = false;
    setting.targetWriterValueSchemaId = 7;
    setting.valueSchema = NAME_RECORD_V1_SCHEMA;
    setting.valueSchemaString = NAME_RECORD_V1_SCHEMA.toString();

    vpj.validateAndRetrieveValueSchemas(mockClient, setting, false);
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Failed to fetch target writer value schema id: 7.*")
  public void testValidateValueSchemaProjectionFailsWhenWriterSchemaFetchErrors() {
    // The supplied target writer value schema id cannot be resolved by the controller.
    ControllerClient mockClient = mock(ControllerClient.class);

    SchemaResponse valueSchemaIdResponse = new SchemaResponse();
    valueSchemaIdResponse.setError("Could not find any registered value schema for the input value schema.");
    doReturn(valueSchemaIdResponse).when(mockClient)
        .getValueSchemaID(eq(TEST_STORE), eq(NAME_RECORD_V2_SCHEMA.toString()));

    SchemaResponse writerSchemaResponse = new SchemaResponse();
    writerSchemaResponse.setError("No value schema found for id: 7");
    doReturn(writerSchemaResponse).when(mockClient).getValueSchema(eq(TEST_STORE), eq(7));

    VenicePushJob vpj = getSpyVenicePushJob(new Properties(), mockClient);
    PushJobSetting setting = vpj.getPushJobSetting();
    setting.storeName = TEST_STORE;
    setting.controllerRetries = 1;
    setting.enableWriteCompute = false;
    setting.targetWriterValueSchemaId = 7;
    setting.valueSchema = NAME_RECORD_V2_SCHEMA;
    setting.valueSchemaString = NAME_RECORD_V2_SCHEMA.toString();

    vpj.validateAndRetrieveValueSchemas(mockClient, setting, false);
  }
}
