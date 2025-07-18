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
  private VeniceHelixAdmin mockAdmin;
  private HelixVeniceClusterResources mockHelixResources;
  private ZkStoreConfigAccessor mockStoreConfigAccessor;
  private TopicManager mockTopicManager;
  private Store mockStore;
  private StoreConfig mockStoreConfig;

  private static final String TEST_CLUSTER = "test-cluster";
  private static final String TEST_STORE = "test-store";

  @BeforeMethod
  public void setUp() {
    mockAdmin = mock(VeniceHelixAdmin.class);
    mockHelixResources = mock(HelixVeniceClusterResources.class);
    mockStoreConfigAccessor = mock(ZkStoreConfigAccessor.class);
    mockTopicManager = mock(TopicManager.class);
    mockStore = mock(Store.class);
    mockStoreConfig = mock(StoreConfig.class);

    // Set up basic mock behavior
    when(mockAdmin.getHelixVeniceClusterResources(TEST_CLUSTER)).thenReturn(mockHelixResources);
    when(mockHelixResources.getStoreConfigAccessor()).thenReturn(mockStoreConfigAccessor);
    when(mockAdmin.getTopicManager()).thenReturn(mockTopicManager);

    // Mock getAllLiveHelixResources to return empty list by default
    when(mockAdmin.getAllLiveHelixResources(TEST_CLUSTER)).thenReturn(Collections.emptyList());
  }

  @Test
  public void testValidateStoreDeletedFullyDeletedSuccess() {
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
  public void testValidateStoreDeletedStoreConfigExistsNotDeleted() {
    // Store config still exists
    when(mockStoreConfigAccessor.getStoreConfig(TEST_STORE)).thenReturn(mockStoreConfig);

    StoreDeletedValidation result =
        StoreDeletionValidationUtils.validateStoreDeleted(mockAdmin, TEST_CLUSTER, TEST_STORE);

    assertFalse(result.isDeleted());
    assertEquals(result.getError(), "Store config still exists in storeConfigRepo.");
  }

  @Test
  public void testValidateStoreDeletedStoreMetadataExistsNotDeleted() {
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
  public void testValidateStoreDeletedPubSubTopicsExistNotDeleted() {
    setupBasicValidationPass();

    // Mock version topic exists
    PubSubTopic mockTopic = mock(PubSubTopic.class);
    when(mockTopic.getName()).thenReturn(TEST_STORE + "_v1");
    when(mockTopic.getStoreName()).thenReturn(TEST_STORE);
    Set<PubSubTopic> topics = new HashSet<>(Arrays.asList(mockTopic));
    when(mockTopicManager.listTopics()).thenReturn(topics);

    StoreDeletedValidation result =
        StoreDeletionValidationUtils.validateStoreDeleted(mockAdmin, TEST_CLUSTER, TEST_STORE);

    assertFalse(result.isDeleted());
    assertEquals(result.getError(), "PubSub topic still exists: " + TEST_STORE + "_v1");
  }

  @Test
  public void testValidateStoreDeletedMainStoreHelixResourceExistsNotDeleted() {
    setupBasicValidationPass();

    // Mock that a Helix resource exists for the store version topic
    List<String> helixResources = Arrays.asList(TEST_STORE + "_v1");
    when(mockAdmin.getAllLiveHelixResources(TEST_CLUSTER)).thenReturn(helixResources);

    StoreDeletedValidation result =
        StoreDeletionValidationUtils.validateStoreDeleted(mockAdmin, TEST_CLUSTER, TEST_STORE);

    assertFalse(result.isDeleted());
    // Now that we're using VeniceHelixAdmin mock, the Helix resource check should work
    assertEquals(result.getError(), "Helix resource still exists: " + TEST_STORE + "_v1");
  }

  @Test
  public void testValidateStoreDeletedSystemStoreHelixResourceExistsNotDeleted() {
    setupBasicValidationPass();

    // Mock system store Helix resource exists
    String metaStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(TEST_STORE);
    List<String> helixResources = Arrays.asList(metaStoreName + "_v1");
    when(mockAdmin.getAllLiveHelixResources(TEST_CLUSTER)).thenReturn(helixResources);

    StoreDeletedValidation result =
        StoreDeletionValidationUtils.validateStoreDeleted(mockAdmin, TEST_CLUSTER, TEST_STORE);

    assertFalse(result.isDeleted());
    // Now the Helix resource check should work and detect the system store resource
    assertEquals(result.getError(), "Helix resource still exists: " + metaStoreName + "_v1");
  }

  @Test
  public void testValidateStoreDeletedExceptionDuringValidationNotDeleted() {
    // Mock exception during store config check
    when(mockStoreConfigAccessor.getStoreConfig(TEST_STORE)).thenThrow(new RuntimeException("Test exception"));

    StoreDeletedValidation result =
        StoreDeletionValidationUtils.validateStoreDeleted(mockAdmin, TEST_CLUSTER, TEST_STORE);

    assertFalse(result.isDeleted());
    assertEquals(result.getError(), "Failed to check store config: Test exception");
  }

  @Test
  public void testValidateStoreDeletedTopicManagerExceptionNotDeleted() {
    setupBasicValidationPass();

    // Mock exception during topic listing
    when(mockTopicManager.listTopics()).thenThrow(new RuntimeException("PubSub connection error"));

    StoreDeletedValidation result =
        StoreDeletionValidationUtils.validateStoreDeleted(mockAdmin, TEST_CLUSTER, TEST_STORE);

    assertFalse(result.isDeleted());
    // With the optimization, the exception occurs during the centralized listTopics call
    assertEquals(result.getError(), "Failed to list topics: PubSub connection error");
  }

  @Test
  public void testIsStoreRelatedTopicVersionTopicTrue() {
    // Create mock PubSubTopic for version topics
    PubSubTopic mockTopic1 = mock(PubSubTopic.class);
    when(mockTopic1.getStoreName()).thenReturn(TEST_STORE);
    when(mockTopic1.getName()).thenReturn(TEST_STORE + "_v1");

    PubSubTopic mockTopic2 = mock(PubSubTopic.class);
    when(mockTopic2.getStoreName()).thenReturn(TEST_STORE);
    when(mockTopic2.getName()).thenReturn(TEST_STORE + "_v123");

    assertTrue(StoreDeletionValidationUtils.isStoreRelatedTopic(mockTopic1, TEST_STORE));
    assertTrue(StoreDeletionValidationUtils.isStoreRelatedTopic(mockTopic2, TEST_STORE));
  }

  @Test
  public void testIsStoreRelatedTopic_RTTopic_True() {
    // Create mock PubSubTopic for RT topics
    PubSubTopic mockTopic1 = mock(PubSubTopic.class);
    when(mockTopic1.getStoreName()).thenReturn(TEST_STORE);
    when(mockTopic1.getName()).thenReturn(TEST_STORE + "_rt");

    PubSubTopic mockTopic2 = mock(PubSubTopic.class);
    when(mockTopic2.getStoreName()).thenReturn(TEST_STORE);
    when(mockTopic2.getName()).thenReturn(TEST_STORE + "_rt_v1");

    assertTrue(StoreDeletionValidationUtils.isStoreRelatedTopic(mockTopic1, TEST_STORE));
    assertTrue(StoreDeletionValidationUtils.isStoreRelatedTopic(mockTopic2, TEST_STORE));
  }

  @Test
  public void testIsStoreRelatedTopicViewTopicTrue() {
    // Create mock PubSubTopic for view topics
    PubSubTopic mockTopic1 = mock(PubSubTopic.class);
    when(mockTopic1.getStoreName()).thenReturn(TEST_STORE);
    when(mockTopic1.getName()).thenReturn(TEST_STORE + "_v1_cc");

    PubSubTopic mockTopic2 = mock(PubSubTopic.class);
    when(mockTopic2.getStoreName()).thenReturn(TEST_STORE);
    when(mockTopic2.getName()).thenReturn(TEST_STORE + "_v1_testView_mv");

    // Test change capture view topic format: storeName_v{version}_cc
    assertTrue(StoreDeletionValidationUtils.isStoreRelatedTopic(mockTopic1, TEST_STORE));

    // Test materialized view topic format: storeName_v{version}_{viewName}_mv
    assertTrue(StoreDeletionValidationUtils.isStoreRelatedTopic(mockTopic2, TEST_STORE));
  }

  @Test
  public void testIsStoreRelatedTopicSystemStoreTopicTrue() {
    String metaStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(TEST_STORE);

    // Create mock PubSubTopic for system store topics
    PubSubTopic mockTopic1 = mock(PubSubTopic.class);
    when(mockTopic1.getStoreName()).thenReturn(metaStoreName);
    when(mockTopic1.getName()).thenReturn(metaStoreName + "_v1");

    PubSubTopic mockTopic2 = mock(PubSubTopic.class);
    when(mockTopic2.getStoreName()).thenReturn(metaStoreName);
    when(mockTopic2.getName()).thenReturn(metaStoreName + "_rt");

    assertTrue(StoreDeletionValidationUtils.isStoreRelatedTopic(mockTopic1, TEST_STORE));
    assertTrue(StoreDeletionValidationUtils.isStoreRelatedTopic(mockTopic2, TEST_STORE));
  }

  @Test
  public void testIsStoreRelatedTopicUnrelatedTopicFalse() {
    // Create mock PubSubTopic for unrelated topics
    PubSubTopic mockTopic1 = mock(PubSubTopic.class);
    when(mockTopic1.getStoreName()).thenReturn("other-store");
    when(mockTopic1.getName()).thenReturn("other-store_v1");

    PubSubTopic mockTopic2 = mock(PubSubTopic.class);
    when(mockTopic2.getStoreName()).thenReturn("unrelated");
    when(mockTopic2.getName()).thenReturn("unrelated-topic");

    PubSubTopic mockTopic3 = mock(PubSubTopic.class);
    when(mockTopic3.getStoreName()).thenReturn(TEST_STORE + "suffix");
    when(mockTopic3.getName()).thenReturn(TEST_STORE + "suffix_v1");

    assertFalse(StoreDeletionValidationUtils.isStoreRelatedTopic(mockTopic1, TEST_STORE));
    assertFalse(StoreDeletionValidationUtils.isStoreRelatedTopic(mockTopic2, TEST_STORE));
    assertFalse(StoreDeletionValidationUtils.isStoreRelatedTopic(mockTopic3, TEST_STORE));
  }

  @Test
  public void testStoreDeletedValidationToString() {
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
