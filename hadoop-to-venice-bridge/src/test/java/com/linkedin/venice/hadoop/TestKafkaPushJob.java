package com.linkedin.venice.hadoop;

import com.linkedin.venice.client.VeniceReader;
import com.linkedin.venice.client.VeniceWriter;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.exceptions.VeniceInconsistentSchemaException;
import com.linkedin.venice.hadoop.exceptions.VeniceSchemaFieldNotFoundException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.serialization.StringSerializer;
import com.linkedin.venice.serialization.VeniceSerializer;
import com.linkedin.venice.utils.*;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;

public class TestKafkaPushJob {
  private static final Logger LOGGER = Logger.getLogger(TestKafkaPushJob.class);
  private static final int TEST_TIMEOUT = 60 * Time.MS_PER_SECOND;

  private VeniceClusterWrapper veniceCluster;

  private static File getTempDataDirectory() {
    String tmpDirectory = System.getProperty("java.io.tmpdir");
    String directoryName = TestUtils.getUniqueString("Venice-Data");
    File dir = new File(tmpDirectory, directoryName).getAbsoluteFile();
    dir.mkdir();
    return dir;
  }

  /**
   * This function is used to generate a small avro file with 'user' schema.
   */
  private void writeSimpleAvroFileWithUserSchema(File parentDir) throws IOException {
    String schemaStr = "{" +
        "  \"namespace\" : \"example.avro\",  " +
        "  \"type\": \"record\",   " +
        "  \"name\": \"User\",     " +
        "  \"fields\": [           " +
        "       { \"name\": \"id\", \"type\": \"string\" },  " +
        "       { \"name\": \"name\", \"type\": \"string\" },  " +
        "       { \"name\": \"age\", \"type\": \"int\" }" +
        "  ] " +
        " } ";
    Schema schema = Schema.parse(schemaStr);
    File file = new File(parentDir, "simple_user.avro");
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(schema, file);

    String name = "test_name_";
    for (int i = 1; i <= 100; ++i) {
      GenericRecord user = new GenericData.Record(schema);
      user.put("id", Integer.toString(i));
      user.put("name", name + i);
      user.put("age", i);
      dataFileWriter.append(user);
    }

    dataFileWriter.close();
  }

  private void writeComplicateAvroFileWithUserSchema(File parentDir, boolean addFieldWithDefaultValue) throws IOException {
    String schemaStr = "{\"namespace\": \"example.avro\",\n" +
        " \"type\": \"record\",\n" +
        " \"name\": \"User\",\n" +
        " \"fields\": [\n" +
        "      { \"name\": \"id\", \"type\": \"string\"},\n" +
        "      {\n" +
        "       \"name\": \"value\",\n" +
        "       \"type\": {\n" +
        "           \"type\": \"record\",\n" +
        "           \"name\": \"ValueRecord\",\n" +
        "           \"fields\" : [\n" +
        "              {\"name\": \"favorite_number\", \"type\": \"int\"}\n";
    if (addFieldWithDefaultValue) {
      schemaStr += ",{\"name\": \"favorite_color\", \"type\": \"string\", \"default\": \"blue\"}\n";
    }
    schemaStr +=
        "           ]\n" +
        "        }\n" +
        "      }\n" +
        " ]\n" +
        "}";
    Schema schema = Schema.parse(schemaStr);
    File file = new File(parentDir, "simple_user.avro");
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(schema, file);

    String name = "test_name_";
    for (int i = 1; i <= 100; ++i) {
      GenericRecord user = new GenericData.Record(schema);
      user.put("id", Integer.toString(i));
      GenericRecord oldValue = new GenericData.Record(schema.getField("value").schema());
      oldValue.put("favorite_number", i);
      if (addFieldWithDefaultValue) {
        oldValue.put("favorite_color", "red");
      }
      user.put("value", oldValue);
      dataFileWriter.append(user);
    }

    dataFileWriter.close();
  }


  private void writeSimpleAvroFileWithDifferentUserSchema(File parentDir) throws IOException {
    String schemaStr = "{" +
        "  \"namespace\" : \"example.avro\",  " +
        "  \"type\": \"record\",   " +
        "  \"name\": \"User\",     " +
        "  \"fields\": [           " +
        "       { \"name\": \"id\", \"type\": \"string\" },  " +
        "       { \"name\": \"name\", \"type\": \"string\" },  " +
        "       { \"name\": \"age\", \"type\": \"int\" },  " +
        "       { \"name\": \"company\", \"type\": \"string\" }  " +
        "  ] " +
        " } ";
    Schema schema = Schema.parse(schemaStr);
    File file = new File(parentDir, "simple_user_with_different_schema.avro");
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(schema, file);

    String name = "test_name_";
    String company = "company_";
    for (int i = 1; i <= 100; ++i) {
      GenericRecord user = new GenericData.Record(schema);
      user.put("id", Integer.toString(i));
      user.put("name", name + i);
      user.put("age", i);
      user.put("company", company + i);
      dataFileWriter.append(user);
    }

    dataFileWriter.close();
  }

  @BeforeMethod
  public void setUp() {
    veniceCluster = ServiceFactory.getVeniceCluster();
  }

  @AfterMethod
  public void cleanUp() {
    if (veniceCluster != null) {
      veniceCluster.close();
    }
  }

  private VeniceReader<String, String> initVeniceReader(String storeName) {
    VeniceProperties clientProps = new PropertyBuilder()
        .put(KAFKA_BOOTSTRAP_SERVERS, veniceCluster.getKafka().getAddress())
        .put(ZOOKEEPER_ADDRESS, veniceCluster.getZk().getAddress())
        .put(CLUSTER_NAME, veniceCluster.getClusterName()).build();
    VeniceSerializer keySerializer = new StringSerializer();
    VeniceSerializer valueSerializer = new StringSerializer();
    VeniceReader veniceReader = new VeniceReader<String, String>(clientProps, storeName, keySerializer, valueSerializer);
    veniceReader.init();

    return veniceReader;
  }

  private Properties setupDefaultProps(String inputDirPath) {
    Properties props = new Properties();
    props.put(KafkaPushJob.VENICE_ROUTER_URL_PROP, "http://" + veniceCluster.getVeniceRouter().getAddress());
    props.put(KafkaPushJob.KAFKA_URL_PROP, veniceCluster.getKafka().getAddress());
    props.put(KafkaPushJob.KAFKA_ZOOKEEPER_PROP, veniceCluster.getZk().getAddress());
    props.put(KafkaPushJob.VENICE_CLUSTER_NAME_PROP, veniceCluster.getClusterName());
    props.put(KafkaPushJob.VENICE_STORE_NAME_PROP, "user");
    props.put(KafkaPushJob.VENICE_STORE_OWNERS_PROP, "test@linkedin.com");
    props.put(KafkaPushJob.INPUT_PATH_PROP, inputDirPath);
    props.put(KafkaPushJob.AVRO_KEY_FIELD_PROP, "id");
    props.put(KafkaPushJob.AVRO_VALUE_FIELD_PROP, "name");
    props.put(KafkaPushJob.AUTO_CREATE_STORE, "true");
    // No need for a big close timeout in tests. This is just to speed up discovery of certain regressions.
    props.put(VeniceWriter.CLOSE_TIMEOUT_MS, 500);

    return props;
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testRunJob() throws Exception {
    File inputDir = getTempDataDirectory();
    writeSimpleAvroFileWithUserSchema(inputDir);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties props = setupDefaultProps(inputDirPath);

    KafkaPushJob job = new KafkaPushJob("Test push job", props);
    job.run();

    // Verify job properties
    Assert.assertEquals(job.getKafkaTopic(), "user_v1");
    Assert.assertEquals(job.getInputDirectory(), inputDirPath);
    String schema = "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"example.avro\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}";
    Assert.assertEquals(job.getFileSchemaString(), schema);
    Assert.assertEquals(job.getKeySchemaString(), "\"string\"");
    Assert.assertEquals(job.getValueSchemaString(), "\"string\"");
    Assert.assertEquals(job.getInputFileDataSize(), 3872);

    // Verify the data in Venice Store
    String storeName = job.getKafkaTopic();
    VeniceReader reader = initVeniceReader(storeName);

    for (int i = 1; i <= 100; ++i) {
      Assert.assertEquals(reader.get(Integer.toString(i)), "test_name_" + i);
    }
    Assert.assertNull(reader.get("101"));
  }

  @Test(timeOut = TEST_TIMEOUT,
      expectedExceptions = VeniceInconsistentSchemaException.class)
  public void testRunJobWithInputHavingDifferentSchema() throws Exception {
    File inputDir = getTempDataDirectory();
    writeSimpleAvroFileWithUserSchema(inputDir);
    writeSimpleAvroFileWithDifferentUserSchema(inputDir);

    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties props = setupDefaultProps(inputDirPath);

    KafkaPushJob job = new KafkaPushJob("Test push job", props);
    job.run();

    // No need for asserts, because we are expecting an exception to be thrown!
  }

  @Test(expectedExceptions = VeniceSchemaFieldNotFoundException.class, expectedExceptionsMessageRegExp = ".*key field: id1 is not found.*")
  public void testRunJobWithInvalidKeyField() throws Exception {
    File inputDir = getTempDataDirectory();
    writeSimpleAvroFileWithUserSchema(inputDir);
    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties props = setupDefaultProps(inputDirPath);
    // Override with not-existing key field
    props.put(KafkaPushJob.AVRO_KEY_FIELD_PROP, "id1");

    KafkaPushJob job = new KafkaPushJob("Test push job", props);
    job.run();

    // No need for asserts, because we are expecting an exception to be thrown!
  }

  @Test(timeOut = TEST_TIMEOUT,
      expectedExceptions = VeniceSchemaFieldNotFoundException.class,
      expectedExceptionsMessageRegExp = ".*value field: name1 is not found.*")
  public void testRunJobWithInvalidValueField() throws Exception {
    File inputDir = getTempDataDirectory();
    writeSimpleAvroFileWithUserSchema(inputDir);

    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties props = setupDefaultProps(inputDirPath);
    // Override with not-existing value field
    props.put(KafkaPushJob.AVRO_VALUE_FIELD_PROP, "name1");

    KafkaPushJob job = new KafkaPushJob("Test push job", props);
    job.run();

    // No need for asserts, because we are expecting an exception to be thrown!
  }

  @Test(timeOut = TEST_TIMEOUT,
      expectedExceptions = VeniceException.class,
      expectedExceptionsMessageRegExp = ".*should not have sub directory: sub-dir.*")
  public void testRunJobWithSubDirInInputDir() throws Exception {
    File inputDir = getTempDataDirectory();
    writeSimpleAvroFileWithUserSchema(inputDir);
    // Create sub directory
    File subDir = new File(inputDir, "sub-dir");
    subDir.mkdir();

    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties props = setupDefaultProps(inputDirPath);

    KafkaPushJob job = new KafkaPushJob("Test push job", props);
    job.run();

    // No need for asserts, because we are expecting an exception to be thrown!
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testRunJobByPickingUpLatestFolder() throws Exception {
    File inputDir = getTempDataDirectory();
    // Create two folders, and the latest folder with the input data file
    File oldFolder = new File(inputDir, "v1");
    oldFolder.mkdir();
    //oldFolder.setLastModified(System.currentTimeMillis() - 600 * 1000);
    // Right now, the '#LATEST' tag is picking up the latest one sorted by file name instead of the latest modified one
    File newFolder = new File(inputDir, "v2");
    newFolder.mkdir();
    writeSimpleAvroFileWithUserSchema(newFolder);
    String inputDirPath = "file:" + inputDir.getAbsolutePath() + "/#LATEST";
    Properties props = setupDefaultProps(inputDirPath);

    KafkaPushJob job = new KafkaPushJob("Test push job", props);
    job.run();

    // Verify job properties
    Assert.assertEquals(job.getKafkaTopic(), "user_v1");
    Assert.assertEquals(job.getInputDirectory(), "file:" + newFolder.getAbsolutePath());
    String schema = "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"example.avro\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}";
    Assert.assertEquals(job.getFileSchemaString(), schema);
    Assert.assertEquals(job.getKeySchemaString(), "\"string\"");
    Assert.assertEquals(job.getValueSchemaString(), "\"string\"");
    Assert.assertEquals(job.getInputFileDataSize(), 3872);

    // Verify the data in Venice Store
    String storeName = job.getKafkaTopic();
    VeniceReader reader = initVeniceReader(storeName);

    for (int i = 1; i <= 100; ++i) {
      Assert.assertEquals("test_name_" + i, reader.get(Integer.toString(i)));
    }
    Assert.assertNull(reader.get("101"));
  }

  @Test(timeOut = TEST_TIMEOUT,
      expectedExceptions = VeniceException.class,
      expectedExceptionsMessageRegExp = ".*Fail to validate/create key schema.*")
  public void testRunJobMultipleTimesWithDifferentKeySchemaConfig() throws Exception {
    File inputDir = getTempDataDirectory();
    writeSimpleAvroFileWithUserSchema(inputDir);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties props = setupDefaultProps(inputDirPath);
    String jobName = "Test push job";

    KafkaPushJob job = new KafkaPushJob(jobName, props);
    job.run();

    // Verify the data in Venice Store
    String storeName = job.getKafkaTopic();
    VeniceReader reader = initVeniceReader(storeName);

    // Job should success
    for (int i = 1; i <= 100; ++i) {
      Assert.assertEquals(reader.get(Integer.toString(i)), "test_name_" + i);
    }
    Assert.assertNull(reader.get("101"));

    // Run job with different key schema (from 'string' to 'int')
    props.setProperty(KafkaPushJob.AVRO_KEY_FIELD_PROP, "age");
    job = new KafkaPushJob(jobName, props);
    job.run();
  }

  @Test(timeOut = TEST_TIMEOUT,
      expectedExceptions = VeniceException.class,
      expectedExceptionsMessageRegExp = ".*Unable to validate schema for store.*")
  public void testRunJobMultipleTimesWithInCompatibleValueSchemaConfig() throws Exception {
    File inputDir = getTempDataDirectory();
    writeSimpleAvroFileWithUserSchema(inputDir);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties props = setupDefaultProps(inputDirPath);
    String jobName = "Test push job";

    KafkaPushJob job = new KafkaPushJob(jobName, props);
    job.run();

    // Verify the data in Venice Store
    String storeName = job.getKafkaTopic();
    VeniceReader reader = initVeniceReader(storeName);

    // Job should success
    for (int i = 1; i <= 100; ++i) {
      Assert.assertEquals(reader.get(Integer.toString(i)), "test_name_" + i);
    }
    Assert.assertNull(reader.get("101"));

    // Run job with different value schema (from 'string' to 'int')
    props.setProperty(KafkaPushJob.AVRO_VALUE_FIELD_PROP, "age");
    props.setProperty(KafkaPushJob.AUTO_CREATE_STORE, "false");
    job = new KafkaPushJob(jobName, props);
    job.run();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testRunJobMultipleTimesWithCompatibleValueSchemaConfig() throws Exception {
    File inputDir = getTempDataDirectory();
    writeComplicateAvroFileWithUserSchema(inputDir, false);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties props = setupDefaultProps(inputDirPath);
    String jobName = "Test push job";
    props.setProperty(KafkaPushJob.AVRO_VALUE_FIELD_PROP, "value");

    KafkaPushJob job = new KafkaPushJob(jobName, props);
    job.run();

    // Verify the data in Venice Store
    String storeName = job.getKafkaTopic();
    VeniceReader reader = initVeniceReader(storeName);

    // Job should success
    for (int i = 1; i <= 100; ++i) {
      Assert.assertNotNull(reader.get(Integer.toString(i)));
    }
    Assert.assertNull(reader.get("101"));

    // Run job with different but compatible value schema
    inputDir = getTempDataDirectory();
    writeComplicateAvroFileWithUserSchema(inputDir, true);
    inputDirPath = "file://" + inputDir.getAbsolutePath();
    props = setupDefaultProps(inputDirPath);
    props.setProperty(KafkaPushJob.AVRO_VALUE_FIELD_PROP, "value");
    job = new KafkaPushJob(jobName, props);
    job.run();
  }
}
