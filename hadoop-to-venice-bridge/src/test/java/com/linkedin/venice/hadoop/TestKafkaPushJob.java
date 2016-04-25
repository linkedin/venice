package com.linkedin.venice.hadoop;

import com.linkedin.venice.client.VeniceReader;
import com.linkedin.venice.hadoop.exceptions.VeniceInconsistentSchemaException;
import com.linkedin.venice.hadoop.exceptions.VeniceSchemaFieldNotFoundException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.TestUtils;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.serialization.StringSerializer;
import com.linkedin.venice.serialization.VeniceSerializer;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.VeniceProperties;
import junit.framework.Assert;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.log4j.Logger;
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
                "       { \"name\": \"id\", \"type\": \"int\" },  " +
                "       { \"name\": \"name\", \"type\": \"string\" },  " +
                "       { \"name\": \"age\", \"type\": \"int\" }  " +
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
            user.put("id", i);
            user.put("name", name + i);
            user.put("age", i);
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
                "       { \"name\": \"id\", \"type\": \"int\" },  " +
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
            user.put("id", i);
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
        props.put(KafkaPushJob.VENICE_CLUSTER_NAME_PROP, veniceCluster.getClusterName());
        props.put(KafkaPushJob.VENICE_STORE_NAME_PROP, "user");
        props.put(KafkaPushJob.VENICE_STORE_OWNERS_PROP, "test@linkedin.com");
        props.put(KafkaPushJob.INPUT_PATH_PROP, inputDirPath);
        props.put(KafkaPushJob.AVRO_KEY_FIELD_PROP, "id");
        props.put(KafkaPushJob.AVRO_VALUE_FIELD_PROP, "name");

        return props;
    }

    @Test
    public void testRunJob() throws Exception {
        File inputDir = getTempDataDirectory();
        writeSimpleAvroFileWithUserSchema(inputDir);
        String inputDirPath = "file://" + inputDir.getAbsolutePath();
        Properties props = setupDefaultProps(inputDirPath);

        KafkaPushJob job = new KafkaPushJob("Test push job", props);
        job.run();

        // Verify job properties
        Assert.assertEquals("user_v1", job.getKafkaTopic());
        Assert.assertEquals(inputDirPath, job.getInputDirectory());
        String schema = "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"example.avro\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}";
        Assert.assertEquals(schema, job.getFileSchemaString());
        Assert.assertEquals("\"int\"", job.getKeySchemaString());
        Assert.assertEquals("\"string\"", job.getValueSchemaString());
        Assert.assertEquals(3556, job.getInputFileDataSize());

        // Verify the data in Venice Store
        String storeName = job.getKafkaTopic();
        VeniceReader reader = initVeniceReader(storeName);

        for (int i = 1; i <= 100; ++i) {
            Assert.assertEquals("test_name_" + i, reader.get(Integer.toString(i)));
        }
        Assert.assertNull(reader.get("101"));
    }

    @Test(expectedExceptions = VeniceInconsistentSchemaException.class)
    public void testRunJobWithInputHavingDifferentSchema() throws Exception {
        File inputDir = getTempDataDirectory();
        writeSimpleAvroFileWithUserSchema(inputDir);
        writeSimpleAvroFileWithDifferentUserSchema(inputDir);

        // Setup job properties
        String inputDirPath = "file://" + inputDir.getAbsolutePath();
        Properties props = setupDefaultProps(inputDirPath);

        KafkaPushJob job = new KafkaPushJob("Test push job", props);
        job.run();
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
    }

    @Test(expectedExceptions = VeniceSchemaFieldNotFoundException.class, expectedExceptionsMessageRegExp = ".*value field: name1 is not found.*")
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
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*should not have sub directory: sub-dir.*")
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
    }

    @Test
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
        Assert.assertEquals("user_v1", job.getKafkaTopic());
        Assert.assertEquals("file:" + newFolder.getAbsolutePath(), job.getInputDirectory());
        String schema = "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"example.avro\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}";
        Assert.assertEquals(schema, job.getFileSchemaString());
        Assert.assertEquals("\"int\"", job.getKeySchemaString());
        Assert.assertEquals("\"string\"", job.getValueSchemaString());
        Assert.assertEquals(3556, job.getInputFileDataSize());

        // Verify the data in Venice Store
        String storeName = job.getKafkaTopic();
        VeniceReader reader = initVeniceReader(storeName);

        for (int i = 1; i <= 100; ++i) {
            Assert.assertEquals("test_name_" + i, reader.get(Integer.toString(i)));
        }
        Assert.assertNull(reader.get("101"));
    }
}
