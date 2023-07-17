package com.linkedin.venice.controller.server;

import com.linkedin.venice.controller.MockVeniceAuthorizer;
import com.linkedin.venice.controllerapi.AclResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.utils.Utils;
import java.util.Optional;
import java.util.Properties;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * This contains test cases for new Rest api /update_acl and /get_acl and also /new_store
 */
public class TestAdminSparkServerAcl extends AbstractTestAdminSparkServer {
  MockVeniceAuthorizer authorizerService;

  @BeforeClass
  public void setUp() {
    authorizerService = new MockVeniceAuthorizer();
    super.setUp(true, Optional.of(authorizerService), new Properties());
  }

  @AfterClass
  public void cleanUp() {
    super.cleanUp();
  }

  /**
   * Test the new store (/new_store) can be created with acls being passed in and subsequently acl's can be updated (/update_acl).
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testAclRestApis() {
    String storeName = Utils.getUniqueString("test-store-authorizer");
    String keySchema = "\"string\"";
    String valueSchema = "\"long\"";
    String accessPerm =
        "{\"AccessPermissions\":{\"Read\":[\"user:user1\",\"group:group1\",\"service:app1\"],\"Write\":[\"user:user1\",\"group:group1\",\"service:app1\"]}}";

    // create Store
    NewStoreResponse newStoreResponse =
        controllerClient.createNewStore(storeName, "owner", keySchema, valueSchema, accessPerm);
    Assert.assertFalse(newStoreResponse.isError(), newStoreResponse.getError());
    AclResponse aclResponse = controllerClient.getAclForStore(storeName);
    Assert.assertFalse(aclResponse.isError(), aclResponse.getError());
    Assert.assertEquals(accessPerm, aclResponse.getAccessPermissions());

    // update acl
    String newAccessPerm =
        "{\"AccessPermissions\":{\"Read\":[\"user:user2\",\"group:group1\",\"service:app1\"],\"Write\":[\"user:user2\",\"group:group1\",\"service:app1\"]}}";
    aclResponse = controllerClient.updateAclForStore(storeName, newAccessPerm);
    Assert.assertFalse(aclResponse.isError(), aclResponse.getError());
    aclResponse = controllerClient.getAclForStore(storeName);
    Assert.assertFalse(aclResponse.isError(), aclResponse.getError());
    Assert.assertEquals(newAccessPerm, aclResponse.getAccessPermissions());

    // delete acl
    aclResponse = controllerClient.deleteAclForStore(storeName);
    Assert.assertFalse(aclResponse.isError(), aclResponse.getError());
    aclResponse = controllerClient.getAclForStore(storeName);
    Assert.assertFalse(aclResponse.isError(), aclResponse.getError());
    Assert.assertEquals(aclResponse.getAccessPermissions(), "");
  }

  /**
   * This verifies rest api /get_acl and /update_acl behaves correctly when the store is not found.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testAclRestApiException() {
    String storeName = Utils.getUniqueString("test-store-authorizer-na");

    AclResponse aclResponse = controllerClient.getAclForStore(storeName);
    Assert.assertTrue(aclResponse.isError(), "getAcl for store should fail as the store does not exist");

    String newAccessPerm =
        "{\"AccessPermissions\":{\"Read\":[\"user:user2\",\"group:group1\",\"service:app1\"],\"Write\":[\"user:user2\",\"group:group1\",\"service:app1\"]}}";

    aclResponse = controllerClient.updateAclForStore(storeName, newAccessPerm);
    Assert.assertTrue(aclResponse.isError(), "updateAcl for store should fail as the store does not exist");

    aclResponse = controllerClient.deleteAclForStore(storeName);
    Assert.assertTrue(aclResponse.isError(), "deleteAcl for store should fail as the store does not exist");

  }
}
