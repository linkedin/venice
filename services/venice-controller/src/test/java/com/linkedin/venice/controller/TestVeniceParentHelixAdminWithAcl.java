package com.linkedin.venice.controller;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.when;

import com.linkedin.venice.authorization.AceEntry;
import com.linkedin.venice.authorization.AclBinding;
import com.linkedin.venice.authorization.Method;
import com.linkedin.venice.authorization.Permission;
import com.linkedin.venice.authorization.Principal;
import com.linkedin.venice.authorization.Resource;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.adapter.SimplePubSubProduceResultImpl;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.utils.TestUtils;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * This contains unit test cases for venice acl authorizer class that is used during store creation.
 * Creating a new class as we need to pass MockAuthorizer during cluster setup.
 */
public class TestVeniceParentHelixAdminWithAcl extends AbstractTestVeniceParentHelixAdmin {
  private static final InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
      AvroProtocolDefinition.PARTITION_STATE.getSerializer();

  MockVeniceAuthorizer authorizerService;

  @BeforeMethod
  public void setUp() throws Exception {
    authorizerService = new MockVeniceAuthorizer();
    setupInternalMocks();
  }

  @AfterMethod
  public void cleanupTestCase() {
    super.cleanupTestCase();
  }

  /**
   * This tests if createStore() is able to properly construct the AclBinding object from input json.
   */
  @Test
  public void testStoreCreationWithAuthorization() {
    String storeName = "test-store-authorizer";
    String accessPerm =
        "{\"AccessPermissions\":{\"Read\":[\"user:user1\",\"group:group1\",\"service:app1\"],\"Write\":[\"user:user1\",\"group:group1\",\"service:app1\"]}}";

    AclBinding expectedAB = new AclBinding(new Resource(storeName));
    Principal userp = new Principal("user:user1");
    Principal groupp = new Principal("group:group1");
    Principal servicep = new Principal("service:app1");
    for (Principal p: new Principal[] { userp, groupp, servicep }) {
      AceEntry race = new AceEntry(p, Method.Read, Permission.ALLOW);
      AceEntry wace = new AceEntry(p, Method.Write, Permission.ALLOW);
      expectedAB.addAceEntry(race);
      expectedAB.addAceEntry(wace);
    }

    doReturn(CompletableFuture.completedFuture(new SimplePubSubProduceResultImpl(topicName, partitionId, 1, -1)))
        .when(veniceWriter)
        .put(any(), any(), anyInt());

    String keySchemaStr = "\"string\"";
    String valueSchemaStr = "\"string\"";
    initializeParentAdmin(Optional.of(authorizerService));
    parentAdmin.initStorageCluster(clusterName);
    parentAdmin
        .createStore(clusterName, storeName, "dev", keySchemaStr, valueSchemaStr, false, Optional.of(accessPerm));
    Assert.assertEquals(1, authorizerService.setAclsCounter);
    AclBinding actualAB = authorizerService.describeAcls(new Resource(storeName));
    Assert.assertTrue(isAclBindingSame(expectedAB, actualAB));
  }

  /**
   * This tests if createStore() is able to throw exception and and stop further processing when acl provisioning fails.
   */
  @Test
  public void testStoreCreationWithAuthorizationException() {
    String storeName = "test-store-authorizer";
    // send an invalid json, so that parsing this would generate an exception and thus failing the createStore api to
    // move forward.
    String accessPerm =
        "{\"AccessPermissions\":{\"Read\":[\"user:user1\",\"group:group1\",\"service:app1\"],\"Write\":[\"user:user1\",\"group:group1\",\"service:app1\"],}}";

    doReturn(CompletableFuture.completedFuture(new SimplePubSubProduceResultImpl(topicName, partitionId, 1, -1)))
        .when(veniceWriter)
        .put(any(), any(), anyInt());

    String keySchemaStr = "\"string\"";
    String valueSchemaStr = "\"string\"";
    initializeParentAdmin(Optional.of(authorizerService));
    parentAdmin.initStorageCluster(clusterName);
    Assert.assertThrows(
        VeniceException.class,
        () -> parentAdmin
            .createStore(clusterName, storeName, "dev", keySchemaStr, valueSchemaStr, false, Optional.of(accessPerm)));
    Assert.assertEquals(0, authorizerService.setAclsCounter);
  }

  @Test
  public void testDeleteStoreWithAuthorization() {
    String storeName = "test-store-authorizer-delete";
    String owner = "unittest";
    Store store = TestUtils.createTestStore(storeName, owner, System.currentTimeMillis());
    doReturn(store).when(internalAdmin).getStore(eq(clusterName), eq(storeName));
    doReturn(store).when(internalAdmin).checkPreConditionForDeletion(eq(clusterName), eq(storeName));

    doReturn(CompletableFuture.completedFuture(new SimplePubSubProduceResultImpl(topicName, partitionId, 1, -1)))
        .when(veniceWriter)
        .put(any(), any(), anyInt());

    when(zkClient.readData(zkMetadataNodePath, null)).thenReturn(null)
        .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(1, -1, 1));
    initializeParentAdmin(Optional.of(authorizerService));
    parentAdmin.initStorageCluster(clusterName);
    parentAdmin.deleteStore(clusterName, storeName, 0, true);
    Assert.assertEquals(1, authorizerService.clearAclCounter);
    AclBinding actualAB = authorizerService.describeAcls(new Resource(storeName));
    Assert.assertTrue(isAclBindingSame(new AclBinding(new Resource(storeName)), actualAB));
  }

  /**
   * This tests if updateAcl() is able to update the ACl rules for a store and deleteAcl is able to delete it.
   */
  @Test
  public void testUpdateAndGetAndDeleteAcl() {
    String storeName = "test-store-authorizer";
    String expectedPerm =
        "{\"AccessPermissions\":{\"Read\":[\"user:user2\",\"group:group2\",\"service:app2\"],\"Write\":[\"user:user2\",\"group:group2\",\"service:app2\"]}}";
    initializeParentAdmin(Optional.of(authorizerService));
    parentAdmin.updateAclForStore(clusterName, storeName, expectedPerm);
    Assert.assertEquals(1, authorizerService.setAclsCounter);
    String curPerm = parentAdmin.getAclForStore(clusterName, storeName);
    Assert.assertEquals(expectedPerm, curPerm);

    // add a DENY ace, this will verify getAclForStore always returns ALLOW acls.
    authorizerService
        .addAce(new Resource(storeName), new AceEntry(new Principal("user:denyuser1"), Method.Read, Permission.DENY));
    curPerm = parentAdmin.getAclForStore(clusterName, storeName);
    Assert.assertEquals(expectedPerm, curPerm);
    parentAdmin.deleteAclForStore(clusterName, storeName);
    Assert.assertEquals(1, authorizerService.clearAclCounter);
    AclBinding actualAB = authorizerService.describeAcls(new Resource(storeName));
    Assert.assertTrue(isAclBindingSame(new AclBinding(new Resource(storeName)), actualAB));
    curPerm = parentAdmin.getAclForStore(clusterName, storeName);
    Assert.assertEquals(curPerm, "");
  }

  /**
   * This test exceptions are properly thrown when a updateacl called for non-existent store.
   */
  @Test
  public void testUpdateAclException() {
    String storeName = "test-store-authorizer";
    String expectedPerm =
        "{\"AccessPermissions\":{\"Read\":[\"user:user2\",\"group:group2\",\"service:app2\"],\"Write\":[\"user:user2\",\"group:group2\",\"service:app2\"]}}";

    doThrow(new VeniceNoStoreException(storeName)).when(internalAdmin)
        .checkPreConditionForAclOp(clusterName, storeName);

    when(zkClient.readData(zkMetadataNodePath, null)).thenReturn(new OffsetRecord(partitionStateSerializer))
        .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(1, -1, 1));
    initializeParentAdmin(Optional.of(authorizerService));
    Assert.assertThrows(
        VeniceNoStoreException.class,
        () -> parentAdmin.updateAclForStore(clusterName, storeName, expectedPerm));
    Assert.assertEquals(0, authorizerService.setAclsCounter);
  }

  /**
   * This test exceptions are properly thrown when a getAcl called for non-existent store.
   */
  @Test
  public void testGetAclException() {
    String storeName = "test-store-authorizer";
    doThrow(new VeniceNoStoreException(storeName)).when(internalAdmin)
        .checkPreConditionForAclOp(clusterName, storeName);

    when(zkClient.readData(zkMetadataNodePath, null)).thenReturn(new OffsetRecord(partitionStateSerializer))
        .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(1, -1, 1));
    initializeParentAdmin(Optional.of(authorizerService));
    Assert.assertThrows(VeniceNoStoreException.class, () -> parentAdmin.getAclForStore(clusterName, storeName));
  }

  /**
   * This test exceptions are properly thrown when a deleteAcl called for non-existent store.
   */
  @Test
  public void testDeleteAclException() {
    String storeName = "test-store-authorizer";
    doThrow(new VeniceNoStoreException(storeName)).when(internalAdmin)
        .checkPreConditionForAclOp(clusterName, storeName);

    when(zkClient.readData(zkMetadataNodePath, null)).thenReturn(new OffsetRecord(partitionStateSerializer))
        .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(1, -1, 1));
    initializeParentAdmin(Optional.of(authorizerService));
    Assert.assertThrows(VeniceNoStoreException.class, () -> parentAdmin.deleteAclForStore(clusterName, storeName));
    Assert.assertEquals(0, authorizerService.clearAclCounter);
  }

  private boolean isAclBindingSame(AclBinding ab1, AclBinding ab2) {
    if (!ab1.getResource().equals(ab2.getResource())) {
      return false;
    }

    Collection<AceEntry> aces1 = ab1.getAceEntries();
    Collection<AceEntry> aces2 = ab2.getAceEntries();

    if (aces1.size() != aces2.size()) {
      return false;
    }

    for (AceEntry e1: aces1) {
      boolean match = false;
      for (AceEntry e2: aces2) {
        if (e1.equals(e2)) {
          match = true;
          break;
        }
      }
      if (!match) {
        return false;
      }
    }
    return true;
  }
}
