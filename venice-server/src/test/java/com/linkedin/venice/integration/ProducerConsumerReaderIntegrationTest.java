package com.linkedin.venice.integration;

import com.linkedin.venice.client.VeniceReader;
import com.linkedin.venice.client.VeniceHttpClient;
import com.linkedin.venice.client.VeniceThinClient;
import com.linkedin.venice.client.VeniceWriter;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.TestUtils;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.serialization.StringSerializer;
import com.linkedin.venice.serialization.VeniceSerializer;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.VeniceProperties;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;

/**
 * This class spins up ZK and Kafka, and a complete Venice cluster, and tests that
 * messages produced into Kafka can be read back out of the storage node.
 */
public class ProducerConsumerReaderIntegrationTest {
  private static final Logger LOGGER = Logger.getLogger(ProducerConsumerReaderIntegrationTest.class);

  // Retry config TODO: Refactor the retry code into a re-usable (and less hacky) class
  // Total Thread.sleep() time: 5 seconds
  private static final int MAX_ATTEMPTS = 50;
  private static final int WAIT_TIME_MS = 100;
  // Total wall-clock time: 10 sec
  private static final int MAX_WAIT_TIME = 10000;

  private VeniceClusterWrapper veniceCluster;
  private String storeVersionName;
  private int valueSchemaId;

  // TODO: Make serializers parameterized so we test them all.
  private VeniceWriter<String, String> veniceWriter;
  private VeniceReader<String, String> veniceReader;
  private VeniceThinClient thinClient;

  @BeforeMethod
  public void setUp() throws InterruptedException, ExecutionException {
    veniceCluster = ServiceFactory.getVeniceCluster();

    // Create test store
    VersionCreationResponse creationResponse = veniceCluster.getNewStoreVersion();
    storeVersionName = creationResponse.getKafkaTopic();
    valueSchemaId = creationResponse.getValueSchemaId();
    String routerUrl = "http://" + veniceCluster.getVeniceRouter().getAddress();
    veniceCluster.getVeniceController().setActiveVersion(routerUrl, veniceCluster.getClusterName(), storeVersionName);

    VeniceProperties clientProps =
            new PropertyBuilder().put(KAFKA_BOOTSTRAP_SERVERS, veniceCluster.getKafka().getAddress())
                    .put(ZOOKEEPER_ADDRESS, veniceCluster.getZk().getAddress())
                    .put(CLUSTER_NAME, veniceCluster.getClusterName()).build();

    // TODO: Make serializers parameterized so we test them all.
    VeniceSerializer keySerializer = new StringSerializer();
    VeniceSerializer valueSerializer = new StringSerializer();

    veniceWriter = new VeniceWriter(clientProps, storeVersionName, keySerializer, valueSerializer);
    veniceReader = new VeniceReader<String, String>(clientProps, storeVersionName, keySerializer, valueSerializer);
    veniceReader.init();
    thinClient = new VeniceHttpClient(
        veniceCluster.getVeniceRouter().getHost(),
        veniceCluster.getVeniceRouter().getPort());
  }

  @AfterMethod
  public void cleanUp() {
    thinClient.close();
    if (veniceCluster != null) {
      veniceCluster.close();
    }
  }

  private String logRetryMessage(int attempt, long timeElapsed, String problem, boolean willTryAgain) {
    return "Got " + problem + " from VeniceReader. " +
        "Attempt #" + attempt + "/" + MAX_ATTEMPTS +
        ". Elapsed time: " + timeElapsed + "/" + MAX_WAIT_TIME +  "ms. " +
        (willTryAgain ? "Will try again in " + WAIT_TIME_MS + "ms." : "Aborting");
  }

  private void handleRetry(int attempt, long timeElapsed, String problem, Consumer<String> failureLambda) throws InterruptedException {
    if (attempt == MAX_ATTEMPTS || timeElapsed > MAX_WAIT_TIME) {
      failureLambda.accept(logRetryMessage(attempt, timeElapsed, problem, false));
    } else {
      LOGGER.info(logRetryMessage(attempt, timeElapsed, problem, true));
      Thread.sleep(WAIT_TIME_MS);
    }
  }

  interface TestLambda {
    /**
     * @param attempt number of attempts so far
     * @param timeElapsed wall-clock time elapsed since before the first attempt
     * @return true if execution should terminate successfully, false if we should retry
     * @throws Exception if the test should error out immediately
     */
    boolean execute(int attempt, long timeElapsed) throws Exception;
  }

  /**
   * Provides a generic way of testing code in a loop.
   *
   * The runtime is capped by {@value #MAX_ATTEMPTS} executions and
   * {@value #MAX_WAIT_TIME} wall-clock time.
   *
   * @param testLambda the code to execute inside the loop
   * @return true the first time testLambda returns true, or
   *         false if it never returns true within the allotted retry amount/time
   * @throws Exception thrown by testLambda, if any
   */
  private boolean retry(TestLambda testLambda) throws Exception {
    long startTime = System.currentTimeMillis();
    long timeElapsed = 0;
    for (int attempt = 1;
         attempt <= MAX_ATTEMPTS && timeElapsed < MAX_WAIT_TIME;
         attempt++, timeElapsed = System.currentTimeMillis() - startTime) {
      try {
        if (testLambda.execute(attempt, timeElapsed)) {
          return true;
        }
      } catch (VeniceException e) {
        // TODO: Change to proper exception types once the VeniceReader and other components are changed accordingly.
        handleRetry(attempt, timeElapsed, e.getClass().getSimpleName(), (message) -> {throw new VeniceException(message, e);} );
      }
    }
    return false;
  }

  @Test(enabled = true) // Sometimes breaks in Gradle... Arrrgh...
  public void testEndToEndProductionAndReading() throws Exception {
    String key = TestUtils.getUniqueString("key");
    String value = TestUtils.getUniqueString("value");

    // TODO: Refactor the retry code into a re-usable (and less hacky) class


    String initialValue = veniceReader.get(key);
    Assert.assertNull(initialValue, "The test environment is not pristine! Key '" + key + "' already exists!");

    // Insert test record and wait synchronously for it to succeed
    veniceWriter.put(key, value, valueSchemaId).get();

    // Read from the storage node
    // This may fail non-deterministically, if the storage node is not done consuming yet, hence the retries.
    Assert.assertTrue(retry((attempt, timeElapsed) -> {
      String newValue = veniceReader.get(key);
      if (newValue == null) {
        handleRetry(attempt, timeElapsed, "null value", (message) -> Assert.fail(message));
        return false;
      } else {
        Assert.assertEquals(newValue, value, "The key '" + key + "' does not contain the expected value!");
        LOGGER.info("Successfully completed the single record end-to-end test (:");
        return true;
      }
    }), "Not able to retrieve key '" + key + "' which was written into Venice!");

    // Read from the router
    String storeName = Version.parseStoreFromKafkaTopicName(storeVersionName);
    byte[] thinClientValueBytes = thinClient.get(storeName, key.getBytes(StandardCharsets.UTF_8)).get();
    Assert.assertEquals(new String(thinClientValueBytes, StandardCharsets.UTF_8), value);
  }

  // TODO: Add tests with more complex scenarios (multiple records, record overwrites, multiple partitions, multiple storage nodes, etc.)
}
