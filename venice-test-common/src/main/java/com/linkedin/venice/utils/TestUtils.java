package com.linkedin.venice.utils;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.controller.VeniceControllerConfig;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.helix.HelixInstanceConverter;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.kafka.consumer.VeniceConsumerFactory;
import com.linkedin.venice.kafka.protocol.state.IncrementalPush;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.offsets.OffsetRecord;

import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import javax.swing.text.html.Option;
import javax.validation.constraints.NotNull;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.log4j.Logger;


/**
 * General-purpose utility functions for tests.
 */
public class TestUtils {
  private static final Logger LOGGER = Logger.getLogger(TestUtils.class);

  /** In milliseconds */
  private static final int WAIT_TIME_FOR_NON_DETERMINISTIC_ACTIONS = 30;
  private static final int MAX_WAIT_TIME_FOR_NON_DETERMINISTIC_ACTIONS = WAIT_TIME_FOR_NON_DETERMINISTIC_ACTIONS * 100;

  public static String getUniqueString(String base) {
    return base + "-" + System.currentTimeMillis() + "-" + RandomGenUtils.getRandomIntWithIn(Integer.MAX_VALUE);
  }

  /**
   * To be used for tests when we need to wait for an async operation to complete.  Pass a timeout, and a labmda
   * for checking if the operation is complete.
   *
   * @param timeout amount of time to wait
   * @param timeoutUnits {@link TimeUnit} for the {@param timeout}
   * @param conditionToWaitFor A {@link BooleanSupplier} which should execute the non-deterministic action and
   *                           return true if it is successful, false otherwise.
   */
  public static void waitForNonDeterministicCompletion(long timeout, TimeUnit timeoutUnits, BooleanSupplier conditionToWaitFor){
    long timeoutTime = System.currentTimeMillis() + timeoutUnits.toMillis(timeout);
    while (!conditionToWaitFor.getAsBoolean()){
      if (System.currentTimeMillis() > timeoutTime){
        throw new RuntimeException("Operation did not complete in time");
      }
      Utils.sleep(WAIT_TIME_FOR_NON_DETERMINISTIC_ACTIONS);
    }
  }

  /**
   * To be used for tests when we need to wait for an async operation to complete. Pass a timeout, and a labmda
   * for checking if the operation is complete.
   *
   * @param timeout amount of time to wait
   * @param timeoutUnits {@link TimeUnit} for the {@param timeout}
   * @param assertionToWaitFor A {@link NonDeterministicAssertion} which should simply execute without exception
   *                           if it is successful, or throw an {@link AssertionError} otherwise.
   * @throws AssertionError throws the exception thrown by the {@link NonDeterministicAssertion} if the maximum
   *                        wait time has been exceeded.
   */
  public static void waitForNonDeterministicAssertion(long timeout,
                                                      TimeUnit timeoutUnits,
                                                      NonDeterministicAssertion assertionToWaitFor) throws AssertionError {
    waitForNonDeterministicAssertion(timeout, timeoutUnits, false, assertionToWaitFor);
  }

  public static void waitForNonDeterministicAssertion(long timeout,
                                                      TimeUnit timeoutUnits,
                                                      boolean exponentialBackOff,
                                                      NonDeterministicAssertion assertionToWaitFor) throws AssertionError {
    long timeoutTime = System.currentTimeMillis() + timeoutUnits.toMillis(timeout);
    long waitTime = WAIT_TIME_FOR_NON_DETERMINISTIC_ACTIONS;
    while (true) {
      try {
        assertionToWaitFor.execute();
        return;
      } catch (AssertionError ae) {
        if (System.currentTimeMillis() > timeoutTime) {
          throw ae;
        } else {
          LOGGER.info("waitForNonDeterministicAssertion caught an AssertionError. Will retry again in: " + waitTime
              + " ms. Assertion message: " + ae.getMessage());
        }
        Utils.sleep(waitTime);
        if (exponentialBackOff) {
          waitTime = Math.min(waitTime * 2, MAX_WAIT_TIME_FOR_NON_DETERMINISTIC_ACTIONS);
        }
      }
    }
  }

  public static Store createTestStore(@NotNull String name, @NotNull String owner, long createdTime) {
      return new Store(name, owner, createdTime, PersistenceType.IN_MEMORY, RoutingStrategy.CONSISTENT_HASH,
          ReadStrategy.ANY_OF_ONLINE, OfflinePushStrategy.WAIT_ALL_REPLICAS);
  }

  public interface NonDeterministicAssertion {
    void execute() throws AssertionError;
  }

  public static SafeHelixManager getParticipant(String cluster, String nodeId, String zkAddress, int httpPort, String stateModelDef) {
    MockTestStateModelFactory stateModelFactory = new MockTestStateModelFactory();
    return getParticipant(cluster, nodeId, zkAddress, httpPort, stateModelFactory, stateModelDef);
  }

  public static SafeHelixManager getParticipant(String cluster, String nodeId, String zkAddress, int httpPort,
      StateModelFactory<StateModel> stateModelFactory, String stateModelDef) {
    SafeHelixManager participant = new SafeHelixManager(
        HelixManagerFactory.getZKHelixManager(cluster, nodeId, InstanceType.PARTICIPANT, zkAddress));
    participant.getStateMachineEngine()
        .registerStateModelFactory(stateModelDef,
            stateModelFactory);
    participant.setLiveInstanceInfoProvider(
        () -> HelixInstanceConverter.convertInstanceToZNRecord(new Instance(nodeId, Utils.getHostName(), httpPort)));
    return participant;
  }

  public static OffsetRecord getOffsetRecord(long currentOffset) {
    return getOffsetRecord(currentOffset, Optional.<Long>empty());
  }

  public static OffsetRecord getOffsetRecord(long currentOffset, boolean complete) {
    return getOffsetRecord(currentOffset, complete ? Optional.of(1000L) : Optional.of(0L));
  }

  public static OffsetRecord getOffsetRecord(long currentOffset, Optional<Long> endOfPushOffset) {
    OffsetRecord offsetRecord = new OffsetRecord();
    offsetRecord.setOffset(currentOffset);
    if (endOfPushOffset.isPresent()) {
      offsetRecord.endOfPushReceived(endOfPushOffset.get());
    }
    return offsetRecord;
  }

  public static OffsetRecord getOffsetRecord(long currentOffset, boolean complete, String incrementalPushVersion) {
    OffsetRecord offsetRecord = getOffsetRecord(currentOffset, complete);
    IncrementalPush incrementalPush = new IncrementalPush();
    incrementalPush.version = incrementalPushVersion;
    offsetRecord.setIncrementalPush(incrementalPush);
    return offsetRecord;
  }

  public static VeniceControllerMultiClusterConfig getMultiClusterConfigFromOneCluster(
      VeniceControllerConfig controllerConfig) {
    Map<String,VeniceControllerConfig> configMap = new HashMap<>();
    configMap.put(controllerConfig.getClusterName(),controllerConfig);
    return new VeniceControllerMultiClusterConfig(configMap);
  }

  public static String getClusterToDefaultD2String(String cluster) {
    return cluster + ":" + D2TestUtils.DEFAULT_TEST_SERVICE_NAME;
  }

  public static VeniceTestWriterFactory getVeniceTestWriterFactory(String kafakBootstrapServer){
    Properties properties =new Properties();
    properties.put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, kafakBootstrapServer);
    return new VeniceTestWriterFactory(properties);
  }


  public static class VeniceTestWriterFactory extends VeniceWriterFactory {
    public VeniceTestWriterFactory(Properties properties) {
      super(properties);
    }

    public <K, V> VeniceWriter<K, V> getVeniceWriter(String topic, VeniceKafkaSerializer<K> keySer,  VeniceKafkaSerializer<V>  valSer) {
      return getVeniceWriter(topic, keySer, valSer, SystemTime.INSTANCE);
    }
  }

  public static VeniceConsumerFactory getVeniceConsumerFactory(String kafakBootstrapServer) {
    return new VeniceConsumerFactory() {
      @Override
      public Properties setupSSL(Properties properties) {
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafakBootstrapServer);
        return properties;
      }
    };
  }
}
