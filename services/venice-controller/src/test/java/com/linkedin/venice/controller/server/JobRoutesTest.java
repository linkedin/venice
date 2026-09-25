package com.linkedin.venice.controller.server;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.status.protocol.PushJobStatusRecordKey;
import com.linkedin.venice.utils.Utils;
import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;
import spark.Request;
import spark.Response;


public class JobRoutesTest {
  private static final Logger LOGGER = LogManager.getLogger(JobRoutesTest.class);

  @Test
  public void testPopulateJobStatus() {
    Admin mockAdmin = mock(VeniceParentHelixAdmin.class);
    doReturn(true).when(mockAdmin).isLeaderControllerFor(anyString());
    doReturn(new Admin.OfflinePushStatusInfo(ExecutionStatus.COMPLETED)).when(mockAdmin)
        .getOffLinePushStatus(anyString(), anyString(), any(), any(), any(), anyBoolean());

    doReturn(2).when(mockAdmin).getReplicationFactor(anyString(), anyString());

    String cluster = Utils.getUniqueString("cluster");
    String store = Utils.getUniqueString("store");
    int version = 5;
    JobRoutes jobRoutes = new JobRoutes(false, Optional.empty());
    JobStatusQueryResponse response =
        jobRoutes.populateJobStatus(cluster, store, version, mockAdmin, Optional.empty(), null, null, false);

    Map<String, String> extraInfo = response.getExtraInfo();
    LOGGER.info("extraInfo: {}", extraInfo);
    Assert.assertNotNull(extraInfo);

    Map<String, String> extraDetails = response.getExtraDetails();
    LOGGER.info("extraDetails: {}", extraDetails);
    Assert.assertNotNull(extraDetails);
  }

  @Test
  public void testSendPushJobDetailsWithSchemaReader() throws Exception {
    int unknownProtocolVersion = AvroProtocolDefinition.PUSH_JOB_DETAILS.getCurrentProtocolVersion() + 1;
    Schema currentSchema = AvroProtocolDefinition.PUSH_JOB_DETAILS.getCurrentProtocolVersionSchema();
    String currentSchemaString = currentSchema.toString();
    int fieldsEndIndex = currentSchemaString.lastIndexOf("]}");
    Schema futureSchema = new Schema.Parser().parse(
        currentSchemaString.substring(0, fieldsEndIndex)
            + ",{\"name\":\"futureField\",\"type\":\"string\",\"default\":\"\"}"
            + currentSchemaString.substring(fieldsEndIndex));

    InternalAvroSpecificSerializer<PushJobDetails> serializer = AvroProtocolDefinition.PUSH_JOB_DETAILS.getSerializer();
    SchemaReader schemaReader = mock(SchemaReader.class);
    doReturn(futureSchema).when(schemaReader).getValueSchema(unknownProtocolVersion);
    serializer.setSchemaReader(schemaReader);

    PushJobDetails pushJobDetails = new PushJobDetails();
    pushJobDetails.clusterName = "test-cluster";
    pushJobDetails.overallStatus = Collections.emptyList();
    pushJobDetails.pushId = "";
    pushJobDetails.failureDetails = "";
    GenericRecord futurePushJobDetails = new GenericData.Record(futureSchema);
    for (Schema.Field field: currentSchema.getFields()) {
      futurePushJobDetails.put(field.name(), pushJobDetails.get(field.pos()));
    }
    futurePushJobDetails.put("futureField", "ignored-by-older-controller");

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    output.write(AvroProtocolDefinition.PUSH_JOB_DETAILS.getMagicByte().get());
    output.write(unknownProtocolVersion);
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(output, null);
    new GenericDatumWriter<GenericRecord>(futureSchema).write(futurePushJobDetails, encoder);
    encoder.flush();

    Admin admin = mock(Admin.class);
    doReturn(true).when(admin).isLeaderControllerFor("test-cluster");
    Request request = mock(Request.class);
    doReturn("test-cluster").when(request).queryParams(ControllerApiConstants.CLUSTER);
    doReturn("test-store").when(request).queryParams(ControllerApiConstants.NAME);
    doReturn("1").when(request).queryParams(ControllerApiConstants.VERSION);
    doReturn(output.toByteArray()).when(request).bodyAsBytes();

    JobRoutes jobRoutes = new JobRoutes(false, Optional.empty(), serializer);
    String responseBody = jobRoutes.sendPushJobDetails(admin).handle(request, mock(Response.class)).toString();
    ControllerResponse controllerResponse =
        AdminSparkServer.OBJECT_MAPPER.readValue(responseBody, ControllerResponse.class);
    Assert.assertFalse(controllerResponse.isError());
    ArgumentCaptor<PushJobDetails> capturedDetails = ArgumentCaptor.forClass(PushJobDetails.class);
    verify(admin).sendPushJobDetails(any(PushJobStatusRecordKey.class), capturedDetails.capture());
    Assert.assertEquals(capturedDetails.getValue().clusterName.toString(), pushJobDetails.clusterName);
    verify(schemaReader).getValueSchema(unknownProtocolVersion);
  }

  @Test
  public void testSendPushJobDetailsProtocolFailureIsBestEffort() throws Exception {
    String clusterName = "test-cluster";
    String storeName = "test-store";
    int storeVersion = 1;
    int unknownProtocolVersion = AvroProtocolDefinition.PUSH_JOB_DETAILS.getCurrentProtocolVersion() + 1;
    byte[] payload = { AvroProtocolDefinition.PUSH_JOB_DETAILS.getMagicByte().get(), (byte) unknownProtocolVersion };

    Admin admin = mock(Admin.class);
    doReturn(true).when(admin).isLeaderControllerFor(clusterName);
    Request request = mock(Request.class);
    doReturn(clusterName).when(request).queryParams(ControllerApiConstants.CLUSTER);
    doReturn(storeName).when(request).queryParams(ControllerApiConstants.NAME);
    doReturn(Integer.toString(storeVersion)).when(request).queryParams(ControllerApiConstants.VERSION);
    doReturn(payload).when(request).bodyAsBytes();
    Response response = mock(Response.class);

    InternalAvroSpecificSerializer<PushJobDetails> serializer = AvroProtocolDefinition.PUSH_JOB_DETAILS.getSerializer();
    SchemaReader schemaReader = mock(SchemaReader.class);
    serializer.setSchemaReader(schemaReader, 1);
    JobRoutes jobRoutes = new JobRoutes(false, Optional.empty(), serializer);
    String responseBody = jobRoutes.sendPushJobDetails(admin).handle(request, response).toString();
    ControllerResponse controllerResponse =
        AdminSparkServer.OBJECT_MAPPER.readValue(responseBody, ControllerResponse.class);

    Assert.assertTrue(controllerResponse.isError());
    Assert.assertTrue(controllerResponse.getError().contains("after 1 attempts"));
    verify(response).status(HttpStatus.SC_OK);
    verify(schemaReader).getValueSchema(unknownProtocolVersion);
    verify(admin, never()).sendPushJobDetails(any(PushJobStatusRecordKey.class), any(PushJobDetails.class));
  }

  @Test
  public void testSendPushJobDetailsValidationFailurePreservesErrorStatus() throws Exception {
    Admin admin = mock(Admin.class);
    doReturn(true).when(admin).isLeaderControllerFor("test-cluster");
    Request request = mock(Request.class, RETURNS_DEEP_STUBS);
    doReturn("test-cluster").when(request).queryParams(ControllerApiConstants.CLUSTER);
    doReturn("test-store").when(request).queryParams(ControllerApiConstants.NAME);
    doReturn("invalid-version").when(request).queryParams(ControllerApiConstants.VERSION);
    Response response = mock(Response.class);

    String responseBody =
        new JobRoutes(false, Optional.empty()).sendPushJobDetails(admin).handle(request, response).toString();
    ControllerResponse controllerResponse =
        AdminSparkServer.OBJECT_MAPPER.readValue(responseBody, ControllerResponse.class);

    Assert.assertTrue(controllerResponse.isError());
    Assert.assertTrue(controllerResponse.getError().contains("must be an integer"));
    verify(response).status(HttpStatus.SC_BAD_REQUEST);
    verify(admin, never()).sendPushJobDetails(any(PushJobStatusRecordKey.class), any(PushJobDetails.class));
  }
}
