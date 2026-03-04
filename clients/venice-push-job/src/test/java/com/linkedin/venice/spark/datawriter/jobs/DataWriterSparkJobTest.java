package com.linkedin.venice.spark.datawriter.jobs;

import static com.linkedin.venice.ConfigKeys.PUBSUB_BROKER_ADDRESS;
import static com.linkedin.venice.ConfigKeys.PUBSUB_SECURITY_PROTOCOL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_TOPIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PARTITION_COUNT;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_CONFIGURATOR_CLASS_CONFIG;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_KEY_PASSWORD_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_KEY_STORE_PASSWORD_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_KEY_STORE_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_TRUST_STORE_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TOPIC_PROP;
import static org.testng.Assert.*;

import com.linkedin.venice.hadoop.PushJobSetting;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.pubsub.api.PubSubSecurityProtocol;
import com.linkedin.venice.spark.SparkConstants;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


public class DataWriterSparkJobTest {
  private static final Logger LOGGER = LogManager.getLogger(DataWriterSparkJobTest.class);
  private DataWriterSparkJob currentTestJob = null;

  @AfterMethod
  public void cleanupSparkSession() {
    if (currentTestJob != null) {
      try {
        if (currentTestJob.getSparkSession() != null) {
          currentTestJob.getSparkSession().stop();
        }
        currentTestJob.close();
      } catch (Exception e) {
        LOGGER.warn("Error during test cleanup", e);
      }
      currentTestJob = null;
    }
  }

  @Test
  public void testKafkaInputDataFramePassesBrokerAddress() {
    ConfigTestSparkJob job = new ConfigTestSparkJob();
    currentTestJob = job;

    PushJobSetting setting = createKafkaInputSetting();
    setting.enableSSL = false;

    job.configure(new VeniceProperties(createDefaultTestProperties()), setting);
    job.getKafkaInputDataFrame();

    SparkSession spark = job.getSparkSession();
    assertEquals(spark.conf().get(PUBSUB_BROKER_ADDRESS), "localhost:9092");
  }

  @Test
  public void testKafkaInputDataFramePassesSSLConfigWhenEnabled() {
    ConfigTestSparkJob job = new ConfigTestSparkJob();
    currentTestJob = job;

    Properties props = createDefaultTestProperties();
    props.setProperty(SSL_CONFIGURATOR_CLASS_CONFIG, "com.linkedin.venice.hadoop.ssl.TempFileSSLConfigurator");
    props.setProperty(SSL_KEY_STORE_PROPERTY_NAME, "li.datavault.identity");
    props.setProperty(SSL_TRUST_STORE_PROPERTY_NAME, "li.datavault.truststore");
    props.setProperty(SSL_KEY_PASSWORD_PROPERTY_NAME, "li.datavault.identity.key.password");
    props.setProperty(SSL_KEY_STORE_PASSWORD_PROPERTY_NAME, "li.datavault.identity.keystore.password");

    PushJobSetting setting = createKafkaInputSetting();
    setting.enableSSL = true;

    job.configure(new VeniceProperties(props), setting);
    job.getKafkaInputDataFrame();

    SparkSession spark = job.getSparkSession();
    assertEquals(
        spark.conf().get(SSL_CONFIGURATOR_CLASS_CONFIG),
        "com.linkedin.venice.hadoop.ssl.TempFileSSLConfigurator");
    assertEquals(spark.conf().get(SSL_KEY_STORE_PROPERTY_NAME), "li.datavault.identity");
    assertEquals(spark.conf().get(SSL_TRUST_STORE_PROPERTY_NAME), "li.datavault.truststore");
    assertEquals(spark.conf().get(SSL_KEY_PASSWORD_PROPERTY_NAME), "li.datavault.identity.key.password");
    assertEquals(spark.conf().get(SSL_KEY_STORE_PASSWORD_PROPERTY_NAME), "li.datavault.identity.keystore.password");
    assertEquals(spark.conf().get(PUBSUB_SECURITY_PROTOCOL), PubSubSecurityProtocol.SSL.name());
  }

  @Test
  public void testKafkaInputDataFrameDoesNotPassSSLConfigWhenDisabled() {
    ConfigTestSparkJob job = new ConfigTestSparkJob();
    currentTestJob = job;

    PushJobSetting setting = createKafkaInputSetting();
    setting.enableSSL = false;

    job.configure(new VeniceProperties(createDefaultTestProperties()), setting);
    job.getKafkaInputDataFrame();

    SparkSession spark = job.getSparkSession();
    try {
      spark.conf().get(SSL_CONFIGURATOR_CLASS_CONFIG);
      // If we get here, the key exists which is wrong
      fail("SSL_CONFIGURATOR_CLASS_CONFIG should not be set when SSL is disabled");
    } catch (java.util.NoSuchElementException e) {
      // Expected - key should not exist
      assertTrue(true);
    }
  }

  private Properties createDefaultTestProperties() {
    Properties props = new Properties();
    props.setProperty(KAFKA_INPUT_TOPIC, "test_store_v1");
    props.setProperty(KAFKA_INPUT_BROKER_URL, "localhost:9092");
    props.setProperty(TOPIC_PROP, "test_store_v1");
    props.setProperty(PARTITION_COUNT, "1");
    props.setProperty("venice.writer.max.record.size.bytes", String.valueOf(Integer.MAX_VALUE));
    props.setProperty("venice.writer.max.size.for.user.payload.per.message.in.bytes", "972800");
    return props;
  }

  private PushJobSetting createKafkaInputSetting() {
    PushJobSetting setting = new PushJobSetting();
    setting.isSourceKafka = true;
    setting.kafkaInputTopic = "test_store_v1";
    setting.kafkaInputBrokerUrl = "localhost:9092";
    setting.repushTTLEnabled = false;
    setting.topic = "test_store_v1";
    setting.kafkaUrl = "localhost:9092";
    setting.partitionerClass = DefaultVenicePartitioner.class.getName();
    setting.partitionCount = 1;
    setting.storeKeySchema = Schema.create(Schema.Type.STRING);
    setting.sourceKafkaInputVersionInfo = new VersionImpl("test_store", 1, "test-push-id");
    return setting;
  }

  private static class ConfigTestSparkJob extends DataWriterSparkJob {
    @Override
    public Dataset<Row> getKafkaInputDataFrame() {
      try {
        return super.getKafkaInputDataFrame();
      } catch (Exception e) {
        // Expected - no real Kafka to connect to. The configs are already set.
        List<Row> emptyRows = Arrays.asList(
            new GenericRowWithSchema(
                new Object[] { "region1", 0, 0L, 0, 1, new byte[0], new byte[0], 0, new byte[0], null },
                SparkConstants.RAW_PUBSUB_INPUT_TABLE_SCHEMA));
        return getSparkSession().createDataFrame(emptyRows, SparkConstants.RAW_PUBSUB_INPUT_TABLE_SCHEMA);
      }
    }
  }
}
