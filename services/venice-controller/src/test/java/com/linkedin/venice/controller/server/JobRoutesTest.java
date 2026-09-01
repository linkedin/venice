package com.linkedin.venice.controller.server;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.utils.Utils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;


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
  public void testDeserializePushJobDetailsWithSchemaReader() throws IOException {
    int unknownProtocolVersion = AvroProtocolDefinition.PUSH_JOB_DETAILS.getCurrentProtocolVersion() + 1;
    Schema currentSchema = AvroProtocolDefinition.PUSH_JOB_DETAILS.getCurrentProtocolVersionSchema();
    String currentSchemaString = currentSchema.toString();
    int fieldsEndIndex = currentSchemaString.lastIndexOf("]}");
    Schema futureSchema = new Schema.Parser()
        .parse(
            currentSchemaString.substring(0, fieldsEndIndex)
                + ",{\"name\":\"futureField\",\"type\":\"string\",\"default\":\"\"}"
                + currentSchemaString.substring(fieldsEndIndex));

    InternalAvroSpecificSerializer<PushJobDetails> serializer =
        AvroProtocolDefinition.PUSH_JOB_DETAILS.getSerializer();
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

    JobRoutes jobRoutes = new JobRoutes(false, Optional.empty(), serializer);
    PushJobDetails deserializedPushJobDetails = jobRoutes.deserializePushJobDetails(output.toByteArray());
    Assert.assertEquals(deserializedPushJobDetails.clusterName.toString(), pushJobDetails.clusterName);
    verify(schemaReader).getValueSchema(unknownProtocolVersion);
  }
}
