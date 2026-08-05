package com.linkedin.venice.spark;

import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_LOCATION;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_PASSWORD;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_TYPE;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEY_PASSWORD;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTSTORE_LOCATION;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTSTORE_PASSWORD;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTSTORE_TYPE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.hadoop.ssl.SSLConfigurator;
import com.linkedin.venice.hadoop.ssl.UserCredentialsFactory;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.StartOfPush;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;


public final class SparkExecutorTestUtils {
  public static final String TEST_KEYSTORE_TYPE = "PKCS12";
  public static final String TEST_TRUSTSTORE_TYPE = "JKS";
  private static final byte[] TEST_DICTIONARY = "spark-ttl-test-dictionary".getBytes(StandardCharsets.UTF_8);
  private static final AtomicInteger SSL_CONFIGURATOR_INVOCATIONS = new AtomicInteger();
  private static final AtomicInteger CONSUMER_FACTORY_INVOCATIONS = new AtomicInteger();
  private static final AtomicInteger DICTIONARY_CONSUMER_INVOCATIONS = new AtomicInteger();

  private SparkExecutorTestUtils() {
  }

  public static void resetInvocations() {
    SSL_CONFIGURATOR_INVOCATIONS.set(0);
    CONSUMER_FACTORY_INVOCATIONS.set(0);
    DICTIONARY_CONSUMER_INVOCATIONS.set(0);
  }

  public static int getSslConfiguratorInvocations() {
    return SSL_CONFIGURATOR_INVOCATIONS.get();
  }

  public static int getConsumerFactoryInvocations() {
    return CONSUMER_FACTORY_INVOCATIONS.get();
  }

  public static int getDictionaryConsumerInvocations() {
    return DICTIONARY_CONSUMER_INVOCATIONS.get();
  }

  public static void withTokenFile(ThrowingRunnable runnable) throws Exception {
    synchronized (SparkExecutorTestUtils.class) {
      File tokenFile = Files.createTempFile("spark-executor-ssl", ".tokens").toFile();
      String tokenFileProperty = UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;
      String previousTokenFile = System.getProperty(tokenFileProperty);
      try {
        Credentials credentials = new Credentials();
        for (int i = 0; i < UserCredentialsFactory.REQUIRED_SECRET_KEY_COUNT; i++) {
          credentials.addSecretKey(new Text("secret-" + i), ("value-" + i).getBytes(StandardCharsets.UTF_8));
        }
        credentials.writeTokenStorageFile(new Path(tokenFile.toURI()), new Configuration());
        System.setProperty(tokenFileProperty, tokenFile.getAbsolutePath());
        runnable.run();
      } finally {
        if (previousTokenFile == null) {
          System.clearProperty(tokenFileProperty);
        } else {
          System.setProperty(tokenFileProperty, previousTokenFile);
        }
        Files.deleteIfExists(tokenFile.toPath());
      }
    }
  }

  public static class RecordingSSLConfigurator implements SSLConfigurator {
    @Override
    public Properties setupSSLConfig(Properties properties, Credentials userCredentials) {
      SSL_CONFIGURATOR_INVOCATIONS.incrementAndGet();
      Properties sslProperties = new Properties();
      sslProperties.setProperty(SSL_KEYSTORE_TYPE, TEST_KEYSTORE_TYPE);
      sslProperties.setProperty(SSL_TRUSTSTORE_TYPE, TEST_TRUSTSTORE_TYPE);
      sslProperties.setProperty(SSL_KEYSTORE_LOCATION, "/tmp/test-keystore");
      sslProperties.setProperty(SSL_TRUSTSTORE_LOCATION, "/tmp/test-truststore");
      sslProperties.setProperty(SSL_KEYSTORE_PASSWORD, "test-password");
      sslProperties.setProperty(SSL_TRUSTSTORE_PASSWORD, "test-password");
      sslProperties.setProperty(SSL_KEY_PASSWORD, "test-password");
      return sslProperties;
    }
  }

  public static class AssertingPubSubConsumerAdapterFactory
      extends PubSubConsumerAdapterFactory<PubSubConsumerAdapter> {
    @Override
    public PubSubConsumerAdapter create(PubSubConsumerAdapterContext context) {
      VeniceProperties properties = context.getVeniceProperties();
      assertEquals(properties.getString(SSL_KEYSTORE_TYPE), TEST_KEYSTORE_TYPE);
      assertEquals(properties.getString(SSL_TRUSTSTORE_TYPE), TEST_TRUSTSTORE_TYPE);
      CONSUMER_FACTORY_INVOCATIONS.incrementAndGet();

      PubSubConsumerAdapter consumer = mock(PubSubConsumerAdapter.class);
      when(consumer.getAssignment()).thenReturn(Collections.emptySet());
      if (context.getConsumerName().contains("DictionaryUtilsConsumer")) {
        DICTIONARY_CONSUMER_INVOCATIONS.incrementAndGet();
        configureDictionaryConsumer(consumer);
      }
      return consumer;
    }

    private static void configureDictionaryConsumer(PubSubConsumerAdapter consumer) {
      AtomicReference<PubSubTopicPartition> subscribedPartition = new AtomicReference<>();
      doAnswer(invocation -> {
        subscribedPartition.set(invocation.getArgument(0));
        return null;
      }).when(consumer).subscribe(any(PubSubTopicPartition.class), any(PubSubPosition.class));
      doAnswer(invocation -> {
        PubSubTopicPartition topicPartition = subscribedPartition.get();
        return Collections
            .singletonMap(topicPartition, Collections.singletonList(createStartOfPushMessage(topicPartition)));
      }).when(consumer).poll(anyLong());
    }

    private static DefaultPubSubMessage createStartOfPushMessage(PubSubTopicPartition topicPartition) {
      StartOfPush startOfPush = new StartOfPush();
      startOfPush.compressionStrategy = CompressionStrategy.ZSTD_WITH_DICT.getValue();
      startOfPush.compressionDictionary = ByteBuffer.wrap(TEST_DICTIONARY);

      ControlMessage controlMessage = new ControlMessage();
      controlMessage.controlMessageType = ControlMessageType.START_OF_PUSH.getValue();
      controlMessage.controlMessageUnion = startOfPush;
      KafkaMessageEnvelope envelope =
          new KafkaMessageEnvelope(MessageType.CONTROL_MESSAGE.getValue(), null, controlMessage, null);
      return new ImmutablePubSubMessage(
          new KafkaKey(MessageType.CONTROL_MESSAGE, new byte[0]),
          envelope,
          topicPartition,
          ApacheKafkaOffsetPosition.of(0),
          0L,
          0);
    }

    @Override
    public String getName() {
      return AssertingPubSubConsumerAdapterFactory.class.getSimpleName();
    }

    @Override
    public void close() throws IOException {
    }
  }

  @FunctionalInterface
  public interface ThrowingRunnable {
    void run() throws Exception;
  }
}
