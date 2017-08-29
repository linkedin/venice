package com.linkedin.venice.utils;

import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.KafkaPushJob;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.samza.VeniceSystemFactory;
import com.linkedin.venice.schema.vson.VsonAvroSchemaAdapter;
import com.linkedin.venice.schema.vson.VsonAvroSerializer;
import com.linkedin.venice.writer.VeniceWriter;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.testng.Assert;

import static com.linkedin.venice.hadoop.KafkaPushJob.*;
import static com.linkedin.venice.samza.VeniceSystemFactory.*;
import static com.linkedin.venice.samza.VeniceSystemFactory.JOB_ID;


public class TestPushUtils {
  public static final String USER_SCHEMA_STRING = "{" +
      "  \"namespace\" : \"example.avro\",  " +
      "  \"type\": \"record\",   " +
      "  \"name\": \"User\",     " +
      "  \"fields\": [           " +
      "       { \"name\": \"id\", \"type\": \"string\" },  " +
      "       { \"name\": \"name\", \"type\": \"string\" },  " +
      "       { \"name\": \"age\", \"type\": \"int\" }" +
      "  ] " +
      " } ";

  public static final String STRING_SCHEMA = "\"string\"";

  public static File getTempDataDirectory() {
    String tmpDirectory = System.getProperty("java.io.tmpdir");
    String directoryName = TestUtils.getUniqueString("Venice-Data");
    File dir = new File(tmpDirectory, directoryName).getAbsoluteFile();
    dir.mkdir();
    dir.deleteOnExit();
    return dir;
  }

  /**
   * This function is used to generate a small avro file with 'user' schema.
   *
   * @param parentDir
   * @return the Schema object for the avro file
   * @throws IOException
   */
  public static Schema writeSimpleAvroFileWithUserSchema(File parentDir) throws IOException {
    Schema schema = Schema.parse(USER_SCHEMA_STRING);
    File file = new File(parentDir, "simple_user.avro");
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(schema, file);

    String name = "test_name_";
    for (int i = 1; i <= 100; ++i) { // DEBUG this should be 100
      GenericRecord user = new GenericData.Record(schema);
      user.put("id", Integer.toString(i));
      user.put("name", name + i);
      user.put("age", i);
      dataFileWriter.append(user);
    }

    dataFileWriter.close();
    return schema;
  }

  public static javafx.util.Pair<Schema, Schema> writeSimpleVsonFile(File parentDir) throws IOException {
    String vsonInteger = "\"int32\"";
    String vsonString = "\"string\"";
    VsonAvroSerializer keySerializer = VsonAvroSerializer.fromSchemaStr(vsonInteger);
    VsonAvroSerializer valueSerializer = VsonAvroSerializer.fromSchemaStr(vsonString);

    SequenceFile.Metadata metadata = new SequenceFile.Metadata();
    metadata.set(new Text("key.schema"), new Text(vsonInteger));
    metadata.set(new Text("value.schema"), new Text(vsonString));

    try(SequenceFile.Writer writer = SequenceFile.createWriter(new Configuration(),
        SequenceFile.Writer.file(new Path(parentDir.toString(), "simple_vson_file")),
        SequenceFile.Writer.keyClass(BytesWritable.class),
        SequenceFile.Writer.valueClass(BytesWritable.class),
        SequenceFile.Writer.metadata(metadata))) {
      for (int i = 0; i < 100; i ++) {
        writer.append(new BytesWritable(keySerializer.toBytes(i)),
            new BytesWritable(valueSerializer.toBytes(String.valueOf(i + 100))));
      }
    }

    return new javafx.util.Pair<>(VsonAvroSchemaAdapter.parse(vsonInteger), VsonAvroSchemaAdapter.parse(vsonString));
  }

  public static Properties defaultH2VProps(VeniceClusterWrapper veniceCluster, String inputDirPath, String storeName) {
    Properties props = new Properties();
    props.put(KafkaPushJob.VENICE_URL_PROP, veniceCluster.getRandomRouterURL());
    props.put(KafkaPushJob.KAFKA_URL_PROP, veniceCluster.getKafka().getAddress());
    props.put(KafkaPushJob.VENICE_CLUSTER_NAME_PROP, veniceCluster.getClusterName());
    props.put(VENICE_STORE_NAME_PROP, storeName);
    props.put(KafkaPushJob.INPUT_PATH_PROP, inputDirPath);
    props.put(KafkaPushJob.KEY_FIELD_PROP, "id");
    props.put(KafkaPushJob.VALUE_FIELD_PROP, "name");
    // No need for a big close timeout in tests. This is just to speed up discovery of certain regressions.
    props.put(VeniceWriter.CLOSE_TIMEOUT_MS, 500);

    return props;
  }

  public static ControllerClient createStoreForJob(VeniceClusterWrapper veniceCluster, Schema recordSchema, Properties props) {
    String keySchemaStr = recordSchema.getField(props.getProperty(KafkaPushJob.KEY_FIELD_PROP)).schema().toString();
    String valueSchemaStr = recordSchema.getField(props.getProperty(KafkaPushJob.VALUE_FIELD_PROP)).schema().toString();

    return createStoreForJob(veniceCluster, keySchemaStr, valueSchemaStr, props);
  }

  public static ControllerClient createStoreForJob(VeniceClusterWrapper veniceCluster,
                                                   String keySchemaStr, String valueSchemaStr, Properties props) {
    ControllerClient controllerClient =
        new ControllerClient(veniceCluster.getClusterName(), props.getProperty(KafkaPushJob.VENICE_URL_PROP));
    NewStoreResponse newStoreResponse = controllerClient.createNewStore(props.getProperty(VENICE_STORE_NAME_PROP),
        "test@linkedin.com", keySchemaStr, valueSchemaStr);

    Assert.assertFalse(newStoreResponse.isError(), "The NewStoreResponse returned an error: " + newStoreResponse.getError());

    ControllerResponse controllerResponse = controllerClient.updateStore(props.getProperty(VENICE_STORE_NAME_PROP), Optional.empty(), Optional.empty(),
        Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(Store.UNLIMITED_STORAGE_QUOTA), Optional.empty(),
        Optional.empty(), Optional.empty());

    Assert.assertFalse(controllerResponse.isError(), "The UpdateStore response returned an error: " + controllerResponse.getError());

    return controllerClient;
  }

  public static void makeStoreHybrid(VeniceClusterWrapper venice, String storeName, long rewindSeconds, long offsetLag) {
    try(ControllerClient controllerClient = new ControllerClient(venice.getClusterName(), venice.getRandomRouterURL())){
      ControllerResponse response = controllerClient.updateStore(storeName, Optional.empty(), Optional.empty(),
          Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
          Optional.of(rewindSeconds), Optional.of(offsetLag));
      if (response.isError()){
        throw new VeniceException(response.getError());
      }
    }
  }

  public static SystemProducer getSamzaProducer(VeniceClusterWrapper venice){
    Map<String, String> samzaConfig = new HashMap<>();
    String configPrefix = SYSTEMS_PREFIX + "venice" + DOT;
    samzaConfig.put(configPrefix + VENICE_PUSH_TYPE, ControllerApiConstants.PushType.STREAM.toString());
    samzaConfig.put(configPrefix + VENICE_URL, venice.getRandomRouterURL());
    samzaConfig.put(configPrefix + VENICE_CLUSTER, venice.getClusterName());
    samzaConfig.put(JOB_ID, TestUtils.getUniqueString("venice-push-id"));
    VeniceSystemFactory factory = new VeniceSystemFactory();
    SystemProducer veniceProducer = factory.getProducer("venice", new MapConfig(samzaConfig), null);
    return veniceProducer;
  }

  /**
   * Generate a streaming record using the provided producer to the specified store
   * key and value schema of the store must both be "string", the record produced is
   * based on the provided recordId
   */
  public static void sendStreamingRecord(SystemProducer producer, String storeName, int recordId){
    OutgoingMessageEnvelope envelope = new OutgoingMessageEnvelope(
        new SystemStream("venice", storeName),
        Integer.toString(recordId),
        "stream_" + recordId);
    producer.send(storeName, envelope);
  }
}
