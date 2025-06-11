package com.linkedin.venice.controller;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.helix.ZkStoreConfigAccessor;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Unit tests for StoreDeletionValidationUtils.
 * Tests all validation scenarios including edge cases and error handling.
 */
public class TestStoreDeletionValidationUtils {
  private Admin mockAdmin;
  private HelixVeniceClusterResources mockHelixResources;
  private ZkStoreConfigAccessor mockStoreConfigAccessor;
  private TopicManager mockTopicManager;
  private Store mockStore;
  private StoreConfig mockStoreConfig;

  private static final String TEST_CLUSTER = "test-cluster";
  private static final String TEST_STORE = "test-store";

  @BeforeMethod
  public void setUp() {
    mockAdmin = mock(Admin.class);
    mockHelixResources = mock(HelixVeniceClusterResources.class);
    mockStoreConfigAccessor = mock(ZkStoreConfigAccessor.class);
    mockTopicManager = mock(TopicManager.class);
    mockStore = mock(Store.class);
    mockStoreConfig = mock(StoreConfig.class);

    // Set up basic mock behavior
    when(mockAdmin.getHelixVeniceClusterResources(TEST_CLUSTER)).thenReturn(mockHelixResources);
    when(mockHelixResources.getStoreConfigAccessor()).thenReturn(mockStoreConfigAccessor);
    when(mockAdmin.getTopicManager()).thenReturn(mockTopicManager);
  }

  @Test
  public void testValidateStoreDeleted_FullyDeleted_Success() {
    // All checks return null/false indicating store is fully deleted
    when(mockStoreConfigAccessor.getStoreConfig(TEST_STORE)).thenReturn(null);
    when(mockAdmin.getStore(TEST_CLUSTER, TEST_STORE)).thenReturn(null);
    when(mockAdmin.getStore(eq(TEST_CLUSTER), anyString())).thenReturn(null); // System stores
    when(mockTopicManager.listTopics()).thenReturn(Collections.emptySet());
    when(mockAdmin.isResourceStillAlive(anyString())).thenReturn(false);

    StoreDeletedValidation result =
        StoreDeletionValidationUtils.validateStoreDeleted(mockAdmin, TEST_CLUSTER, TEST_STORE);

    assertTrue(result.isDeleted());
    assertEquals(result.getClusterName(), TEST_CLUSTER);
    assertEquals(result.getStoreName(), TEST_STORE);
    assertEquals(result.getError(), null);
  }

  @Test
  public void testValidateStoreDeleted_StoreConfigExists_NotDeleted() {
    // Store config still exists
    when(mockStoreConfigAccessor.getStoreConfig(TEST_STORE)).thenReturn(mockStoreConfig);

    StoreDeletedValidation result =
        StoreDeletionValidationUtils.validateStoreDeleted(mockAdmin, TEST_CLUSTER, TEST_STORE);

    assertFalse(result.isDeleted());
    assertEquals(result.getError(), "Store config still exists in storeConfigRepo.");
  }

  @Test
  public void testValidateStoreDeleted_StoreMetadataExists_NotDeleted() {
    // Store config is null but store metadata exists
    when(mockStoreConfigAccessor.getStoreConfig(TEST_STORE)).thenReturn(null);
    when(mockAdmin.getStore(TEST_CLUSTER, TEST_STORE)).thenReturn(mockStore);

    StoreDeletedValidation result =
        StoreDeletionValidationUtils.validateStoreDeleted(mockAdmin, TEST_CLUSTER, TEST_STORE);

    assertFalse(result.isDeleted());
    assertEquals(result.getError(), "Store metadata still exists in storeRepository.");
  }

  @Test
  public void testValidateStoreDeleted_SystemStoreMetadataExists_NotDeleted() {
    // Main store metadata is null but system store metadata exists
    when(mockStoreConfigAccessor.getStoreConfig(TEST_STORE)).thenReturn(null);
    when(mockAdmin.getStore(TEST_CLUSTER, TEST_STORE)).thenReturn(null);

    // Mock system store exists
    String metaStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(TEST_STORE);
    when(mockAdmin.getStore(TEST_CLUSTER, metaStoreName)).thenReturn(mockStore);

    StoreDeletedValidation result =
        StoreDeletionValidationUtils.validateStoreDeleted(mockAdmin, TEST_CLUSTER, TEST_STORE);

    assertFalse(result.isDeleted());
    assertTrue(result.getError().contains("System store metadata still exists"));
    assertTrue(result.getError().contains(metaStoreName));
  }

  @Test
  public void testValidateStoreDeleted_KafkaTopicsExist_NotDeleted() {
    setupBasicValidationPass();

    // Mock version topic exists
    PubSubTopic mockTopic = mock(PubSubTopic.class);
    when(mockTopic.getName()).thenReturn(TEST_STORE + "_v1");
    Set<PubSubTopic> topics = new HashSet<>(Arrays.asList(mockTopic));
    when(mockTopicManager.listTopics()).thenReturn(topics);

    StoreDeletedValidation result =
        StoreDeletionValidationUtils.validateStoreDeleted(mockAdmin, TEST_CLUSTER, TEST_STORE);

    assertFalse(result.isDeleted());
    assertEquals(result.getError(), "Kafka topic still exists: " + TEST_STORE + "_v1");
  }

  @Test
  public void testValidateStoreDeleted_MainStoreHelixResourceExists_NotDeleted() {
    setupBasicValidationPass();

    // Mock version topic exists but is NOT detected by the Kafka topic check
    // This happens when topic name doesn't match our isStoreRelatedTopic logic
    // but still has a Helix resource
    PubSubTopic mockTopic = mock(PubSubTopic.class);
    when(mockTopic.getName()).thenReturn(TEST_STORE + "_v1");
    Set<PubSubTopic> topics = new HashSet<>(Arrays.asList(mockTopic));
    when(mockTopicManager.listTopics()).thenReturn(topics);

    // Mock that the version topic has a Helix resource
    when(mockAdmin.isResourceStillAlive(TEST_STORE + "_v1")).thenReturn(true);

    StoreDeletedValidation result =
        StoreDeletionValidationUtils.validateStoreDeleted(mockAdmin, TEST_CLUSTER, TEST_STORE);

    assertFalse(result.isDeleted());
    // Since Kafka topic check comes first and the topic matches our pattern,
    // it will fail on Kafka topic, not Helix resource
    assertEquals(result.getError(), "Kafka topic still exists: " + TEST_STORE + "_v1");
  }

  @Test
  public void testValidateStoreDeleted_SystemStoreHelixResourceExists_NotDeleted() {
    setupBasicValidationPass();

    // Mock system store version topic exists
    String metaStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(TEST_STORE);
    PubSubTopic mockTopic = mock(PubSubTopic.class);
    when(mockTopic.getName()).thenReturn(metaStoreName + "_v1");
    Set<PubSubTopic> topics = new HashSet<>(Arrays.asList(mockTopic));
    when(mockTopicManager.listTopics()).thenReturn(topics);

    // Mock that the system store version topic has a Helix resource
    when(mockAdmin.isResourceStillAlive(metaStoreName + "_v1")).thenReturn(true);

    StoreDeletedValidation result =
        StoreDeletionValidationUtils.validateStoreDeleted(mockAdmin, TEST_CLUSTER, TEST_STORE);

    assertFalse(result.isDeleted());
    // Since Kafka topic check comes first and the topic matches our pattern,
    // it will fail on Kafka topic, not Helix resource
    assertEquals(result.getError(), "Kafka topic still exists: " + metaStoreName + "_v1");
  }

  @Test
  public void testValidateStoreDeleted_ExceptionDuringValidation_NotDeleted() {
    // Mock exception during store config check
    when(mockStoreConfigAccessor.getStoreConfig(TEST_STORE)).thenThrow(new RuntimeException("Test exception"));

    StoreDeletedValidation result =
        StoreDeletionValidationUtils.validateStoreDeleted(mockAdmin, TEST_CLUSTER, TEST_STORE);

    assertFalse(result.isDeleted());
    assertEquals(result.getError(), "Failed to check store config: Test exception");
  }

  @Test
  public void testValidateStoreDeleted_TopicManagerException_NotDeleted() {
    setupBasicValidationPass();

    // Mock exception during topic listing
    when(mockTopicManager.listTopics()).thenThrow(new RuntimeException("Kafka connection error"));

    StoreDeletedValidation result =
        StoreDeletionValidationUtils.validateStoreDeleted(mockAdmin, TEST_CLUSTER, TEST_STORE);

    assertFalse(result.isDeleted());
    assertEquals(result.getError(), "Failed to check Kafka topics: Kafka connection error");
  }

  @Test
  public void testIsStoreRelatedTopic_VersionTopic_True() {
    List<VeniceSystemStoreType> systemStoreTypes = Arrays.asList(VeniceSystemStoreType.values());

    assertTrue(StoreDeletionValidationUtils.isStoreRelatedTopic(TEST_STORE + "_v1", TEST_STORE, systemStoreTypes));
    assertTrue(StoreDeletionValidationUtils.isStoreRelatedTopic(TEST_STORE + "_v123", TEST_STORE, systemStoreTypes));
  }

  @Test
  public void testIsStoreRelatedTopic_RTTopic_True() {
    List<VeniceSystemStoreType> systemStoreTypes = Arrays.asList(VeniceSystemStoreType.values());

    assertTrue(StoreDeletionValidationUtils.isStoreRelatedTopic(TEST_STORE + "_rt", TEST_STORE, systemStoreTypes));
    assertTrue(StoreDeletionValidationUtils.isStoreRelatedTopic(TEST_STORE + "_rt_v1", TEST_STORE, systemStoreTypes));
  }

  @Test
  public void testIsStoreRelatedTopic_ViewTopic_True() {
    List<VeniceSystemStoreType> systemStoreTypes = Arrays.asList(VeniceSystemStoreType.values());

    assertTrue(
        StoreDeletionValidationUtils.isStoreRelatedTopic(TEST_STORE + "_view_test", TEST_STORE, systemStoreTypes));
  }

  @Test
  public void testIsStoreRelatedTopic_SystemStoreTopic_True() {
    List<VeniceSystemStoreType> systemStoreTypes = Arrays.asList(VeniceSystemStoreType.values());
    String metaStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(TEST_STORE);

    assertTrue(StoreDeletionValidationUtils.isStoreRelatedTopic(metaStoreName + "_v1", TEST_STORE, systemStoreTypes));
    assertTrue(StoreDeletionValidationUtils.isStoreRelatedTopic(metaStoreName + "_rt", TEST_STORE, systemStoreTypes));
  }

  @Test
  public void testIsStoreRelatedTopic_UnrelatedTopic_False() {
    List<VeniceSystemStoreType> systemStoreTypes = Arrays.asList(VeniceSystemStoreType.values());

    assertFalse(StoreDeletionValidationUtils.isStoreRelatedTopic("other-store_v1", TEST_STORE, systemStoreTypes));
    assertFalse(StoreDeletionValidationUtils.isStoreRelatedTopic("unrelated-topic", TEST_STORE, systemStoreTypes));
    assertFalse(
        StoreDeletionValidationUtils.isStoreRelatedTopic(TEST_STORE + "suffix_v1", TEST_STORE, systemStoreTypes));
  }

  @Test
  public void testStoreDeletedValidation_ToString() {
    StoreDeletedValidation deletedResult = new StoreDeletedValidation(TEST_CLUSTER, TEST_STORE);
    assertEquals(deletedResult.toString(), "Store 'test-store' in cluster 'test-cluster' is fully deleted");

    StoreDeletedValidation notDeletedResult = new StoreDeletedValidation(TEST_CLUSTER, TEST_STORE);
    notDeletedResult.setStoreNotDeleted("Store config still exists");
    assertEquals(
        notDeletedResult.toString(),
        "Store 'test-store' in cluster 'test-cluster' is NOT fully deleted: Store config still exists");
  }

  /**
   * Helper method to set up basic validation to pass the first few checks.
   */
  private void setupBasicValidationPass() {
    when(mockStoreConfigAccessor.getStoreConfig(TEST_STORE)).thenReturn(null);
    when(mockAdmin.getStore(eq(TEST_CLUSTER), anyString())).thenReturn(null);
  }
}
