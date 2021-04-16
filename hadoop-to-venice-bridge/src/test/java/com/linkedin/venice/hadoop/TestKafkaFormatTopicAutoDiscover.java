package com.linkedin.venice.hadoop;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.exceptions.UndefinedPropertyException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.venice.hadoop.VenicePushJob.*;
import static org.mockito.Mockito.*;


public class TestKafkaFormatTopicAutoDiscover {
  private static final String JOB_ID = "some-job-ID";
  private static final String STORE_NAME = "store-name";
  private static final String STORE_NAME_2 = "store-name-2";

  @Test (expectedExceptions = UndefinedPropertyException.class, expectedExceptionsMessageRegExp = "Missing required property 'venice.store.name'.")
  public void testMissingStoreNameInConfig() {
    new VenicePushJob(JOB_ID, getJobProperties(Collections.emptyMap()));
  }

  @Test
  public void testNoUserProvidedTopicNameAndSingleColoVersion() {
    final int singleColoCurrentVersion = 1;
    ControllerClient controllerClient = mock(ControllerClient.class);
    Map<String, Integer> coloToVersionMap = Collections.emptyMap(); // Single colo
    StoreResponse storeResponse = getMockStoreResponse(coloToVersionMap, singleColoCurrentVersion);
    when(controllerClient.getStore(STORE_NAME)).thenReturn(storeResponse);

    Map<String, String> overrideProperties = Collections.singletonMap(VENICE_STORE_NAME_PROP, STORE_NAME);
    VenicePushJob venicePushJob = new VenicePushJob(JOB_ID, getJobProperties(overrideProperties), controllerClient);
    String expectedTopicName = Version.composeKafkaTopic(STORE_NAME, singleColoCurrentVersion);
    Assert.assertEquals(venicePushJob.getPushJobSetting().kafkaInputTopic, expectedTopicName);
  }

  @Test
  public void testNoUserProvidedTopicNameAndMultiColoVersion() {
    final int multipleColoCurrentVersion = 1;
    ControllerClient controllerClient = mock(ControllerClient.class);
    Map<String, Integer> coloToVersionMap = new HashMap<>(3);
    coloToVersionMap.put("colo-0", multipleColoCurrentVersion);
    coloToVersionMap.put("colo-1", multipleColoCurrentVersion);
    coloToVersionMap.put("colo-2", multipleColoCurrentVersion);
    StoreResponse storeResponse = getMockStoreResponse(coloToVersionMap, -1);
    when(controllerClient.getStore(STORE_NAME)).thenReturn(storeResponse);

    Map<String, String> overrideProperties = Collections.singletonMap(VENICE_STORE_NAME_PROP, STORE_NAME);
    VenicePushJob venicePushJob = new VenicePushJob(JOB_ID, getJobProperties(overrideProperties), controllerClient);
    String expectedTopicName = Version.composeKafkaTopic(STORE_NAME, multipleColoCurrentVersion);
    Assert.assertEquals(venicePushJob.getPushJobSetting().kafkaInputTopic, expectedTopicName);
  }

  @Test (expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Got current topic version mismatch across multiple colos.*")
  public void testNoUserProvidedTopicNameAndMultiColoVersionMismatch() {
    // Mismatched versions below
    final int multipleColoCurrentVersion1 = 1;
    final int multipleColoCurrentVersion2 = 2;
    ControllerClient controllerClient = mock(ControllerClient.class);
    Map<String, Integer> coloToVersionMap = new HashMap<>(3);
    coloToVersionMap.put("colo-0", multipleColoCurrentVersion1);
    coloToVersionMap.put("colo-1", multipleColoCurrentVersion1);
    coloToVersionMap.put("colo-2", multipleColoCurrentVersion2);
    StoreResponse storeResponse = getMockStoreResponse(coloToVersionMap, -1);
    when(controllerClient.getStore(STORE_NAME)).thenReturn(storeResponse);

    Map<String, String> overrideProperties = Collections.singletonMap(VENICE_STORE_NAME_PROP, STORE_NAME);
    new VenicePushJob(JOB_ID, getJobProperties(overrideProperties), controllerClient);
  }

  @Test
  public void testUserProvidedTopicNameAndMultiColoVersionMismatch() {
    // Mismatched versions below
    final int multipleColoCurrentVersion1 = 1;
    final int multipleColoCurrentVersion2 = 2;
    ControllerClient controllerClient = mock(ControllerClient.class);
    Map<String, Integer> coloToVersionMap = new HashMap<>(3);
    coloToVersionMap.put("colo-0", multipleColoCurrentVersion1);
    coloToVersionMap.put("colo-1", multipleColoCurrentVersion1);
    coloToVersionMap.put("colo-2", multipleColoCurrentVersion2);
    StoreResponse storeResponse = getMockStoreResponse(coloToVersionMap, -1);
    when(controllerClient.getStore(STORE_NAME)).thenReturn(storeResponse);

    final String userProvidedTopicName = Version.composeKafkaTopic(STORE_NAME, 3);
    Map<String, String> overrideProperties = new HashMap<>();
    overrideProperties.put(VENICE_STORE_NAME_PROP, STORE_NAME);
    overrideProperties.put(KAFKA_INPUT_TOPIC, userProvidedTopicName);
    VenicePushJob venicePushJob = new VenicePushJob(JOB_ID, getJobProperties(overrideProperties), controllerClient);
    Assert.assertEquals(venicePushJob.getPushJobSetting().kafkaInputTopic, userProvidedTopicName);
  }

  @Test (expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Store user-provided name mismatch with the derived store name.*")
  public void testUserProvidedTopicNameNotValid() {
    ControllerClient controllerClient = mock(ControllerClient.class);
    StoreResponse storeResponse = getMockStoreResponse(Collections.emptyMap(), -1);
    when(controllerClient.getStore(STORE_NAME)).thenReturn(storeResponse);

    final String userProvidedTopicName = Version.composeKafkaTopic(STORE_NAME_2, 3);
    Map<String, String> overrideProperties = new HashMap<>();
    overrideProperties.put(VENICE_STORE_NAME_PROP, STORE_NAME);
    overrideProperties.put(KAFKA_INPUT_TOPIC, userProvidedTopicName);
    new VenicePushJob(JOB_ID, getJobProperties(overrideProperties), controllerClient);
  }

  @Test
  public void testUserProvidedTopicNameMatchDiscoveredTopicName() {
    final int multipleColoCurrentVersion = 1;
    ControllerClient controllerClient = mock(ControllerClient.class);
    Map<String, Integer> coloToVersionMap = new HashMap<>(3);
    coloToVersionMap.put("colo-0", multipleColoCurrentVersion);
    coloToVersionMap.put("colo-1", multipleColoCurrentVersion);
    coloToVersionMap.put("colo-2", multipleColoCurrentVersion);
    StoreResponse storeResponse = getMockStoreResponse(coloToVersionMap, -1);
    when(controllerClient.getStore(STORE_NAME)).thenReturn(storeResponse);

    final String userProvidedTopicName = Version.composeKafkaTopic(STORE_NAME, multipleColoCurrentVersion);
    Map<String, String> overrideProperties = new HashMap<>();
    overrideProperties.put(VENICE_STORE_NAME_PROP, STORE_NAME);
    overrideProperties.put(KAFKA_INPUT_TOPIC, userProvidedTopicName);
    VenicePushJob venicePushJob = new VenicePushJob(JOB_ID, getJobProperties(overrideProperties), controllerClient);
    String expectedTopicName = userProvidedTopicName;
    Assert.assertEquals(venicePushJob.getPushJobSetting().kafkaInputTopic, expectedTopicName);
  }

  @Test (expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Store user-provided name mismatch with the derived store name.*")
  public void testUserProvidedTopicNameMismatchDiscoveredTopicName() {
    final int multipleColoCurrentVersion = 1;
    ControllerClient controllerClient = mock(ControllerClient.class);
    Map<String, Integer> coloToVersionMap = new HashMap<>(3);
    coloToVersionMap.put("colo-0", multipleColoCurrentVersion);
    coloToVersionMap.put("colo-1", multipleColoCurrentVersion);
    coloToVersionMap.put("colo-2", multipleColoCurrentVersion);
    StoreResponse storeResponse = getMockStoreResponse(coloToVersionMap, -1);
    when(controllerClient.getStore(STORE_NAME)).thenReturn(storeResponse);

    final String userProvidedTopicName = Version.composeKafkaTopic(STORE_NAME_2, multipleColoCurrentVersion);
    Map<String, String> overrideProperties = new HashMap<>();
    overrideProperties.put(VENICE_STORE_NAME_PROP, STORE_NAME);
    overrideProperties.put(KAFKA_INPUT_TOPIC, userProvidedTopicName);
    VenicePushJob venicePushJob = new VenicePushJob(JOB_ID, getJobProperties(overrideProperties), controllerClient);
    String expectedTopicName = userProvidedTopicName;
    Assert.assertEquals(venicePushJob.getPushJobSetting().kafkaInputTopic, expectedTopicName);
  }

  private StoreResponse getMockStoreResponse(Map<String, Integer> coloToVersionMap, int currentVersion) {
    StoreResponse storeResponse = mock(StoreResponse.class);
    StoreInfo storeInfo = mock(StoreInfo.class);
    when(storeInfo.getColoToCurrentVersions()).thenReturn(coloToVersionMap);
    when(storeInfo.getCurrentVersion()).thenReturn(currentVersion);
    when(storeResponse.getStore()).thenReturn(storeInfo);
    return storeResponse;
  }

  private Properties getJobProperties(Map<String, String> overrideConfigs) {
    Properties properties = new Properties();
    overrideConfigs.forEach(properties::setProperty);
    properties.setProperty(SSL_KEY_PASSWORD_PROPERTY_NAME, "something");
    properties.setProperty(SSL_KEY_STORE_PASSWORD_PROPERTY_NAME, "something");
    properties.setProperty(SSL_KEY_STORE_PROPERTY_NAME, "something");
    properties.setProperty(SSL_TRUST_STORE_PROPERTY_NAME, "something");
    properties.setProperty(SOURCE_KAFKA, "true");
    properties.setProperty(KAFKA_INPUT_BROKER_URL, "some-kafka-input-broker-url");
    properties.setProperty(VENICE_URL_PROP, "some-venice-URL");
    return properties;
  }
}
