package com.linkedin.venice.controllerapi.request;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import org.testng.annotations.Test;


public class CreateNewStoreRequestTest {
  private static final String CLUSTER_NAME = "test-cluster";
  private static final String STORE_NAME = "storeName";
  private static final String OWNER = "owner";
  private static final String KEY_SCHEMA = "int";
  private static final String VALUE_SCHEMA = "string";
  private static final String ACCESS_PERMISSION =
      "{\"owner\":\"owner\",\"readers\":[\"reader1\",\"reader2\"],\"writers\":[\"writer1\",\"writer2\"]}";

  @Test
  public void testValidInputs() {
    boolean isSystemStore = false;

    CreateNewStoreRequest request = new CreateNewStoreRequest(
        CLUSTER_NAME,
        STORE_NAME,
        OWNER,
        KEY_SCHEMA,
        VALUE_SCHEMA,
        ACCESS_PERMISSION,
        isSystemStore);
    assertNotNull(request);
    assertEquals(request.getClusterName(), CLUSTER_NAME);
    assertEquals(request.getStoreName(), STORE_NAME);
    assertEquals(request.getOwner(), OWNER);
    assertEquals(request.getKeySchema(), KEY_SCHEMA);
    assertEquals(request.getValueSchema(), VALUE_SCHEMA);
    assertEquals(request.getAccessPermissions(), ACCESS_PERMISSION);
    assertEquals(request.isSystemStore(), isSystemStore);
  }

  @Test
  public void testInvalidClusterName() {
    IllegalArgumentException exception1 = expectThrows(
        IllegalArgumentException.class,
        () -> new CreateNewStoreRequest(null, STORE_NAME, OWNER, KEY_SCHEMA, VALUE_SCHEMA, null, false));
    assertEquals("The request is missing the cluster_name, which is a mandatory field.", exception1.getMessage());

    IllegalArgumentException exception2 = expectThrows(
        IllegalArgumentException.class,
        () -> new CreateNewStoreRequest("", STORE_NAME, OWNER, KEY_SCHEMA, VALUE_SCHEMA, null, false));
    assertEquals("The request is missing the cluster_name, which is a mandatory field.", exception2.getMessage());
  }

  @Test
  public void testInvalidStoreName() {
    IllegalArgumentException exception1 = expectThrows(
        IllegalArgumentException.class,
        () -> new CreateNewStoreRequest(CLUSTER_NAME, null, OWNER, KEY_SCHEMA, VALUE_SCHEMA, null, false));
    assertEquals("The request is missing the store_name, which is a mandatory field.", exception1.getMessage());

    IllegalArgumentException exception2 = expectThrows(
        IllegalArgumentException.class,
        () -> new CreateNewStoreRequest(CLUSTER_NAME, "", OWNER, KEY_SCHEMA, VALUE_SCHEMA, null, false));
    assertEquals("The request is missing the store_name, which is a mandatory field.", exception2.getMessage());
  }

  @Test
  public void testInvalidKeySchema() {
    IllegalArgumentException exception1 = expectThrows(
        IllegalArgumentException.class,
        () -> new CreateNewStoreRequest(CLUSTER_NAME, STORE_NAME, OWNER, null, VALUE_SCHEMA, null, false));
    assertEquals("The request is missing the Key schema, which is a mandatory field.", exception1.getMessage());

    IllegalArgumentException exception2 = expectThrows(
        IllegalArgumentException.class,
        () -> new CreateNewStoreRequest(CLUSTER_NAME, STORE_NAME, OWNER, "", VALUE_SCHEMA, null, false));
    assertEquals("The request is missing the Key schema, which is a mandatory field.", exception2.getMessage());
  }

  @Test
  public void testInvalidValueSchema() {
    IllegalArgumentException exception1 = expectThrows(
        IllegalArgumentException.class,
        () -> new CreateNewStoreRequest(CLUSTER_NAME, STORE_NAME, OWNER, KEY_SCHEMA, null, null, false));
    assertEquals("The request is missing the Value schema, which is a mandatory field.", exception1.getMessage());

    IllegalArgumentException exception2 = expectThrows(
        IllegalArgumentException.class,
        () -> new CreateNewStoreRequest(CLUSTER_NAME, STORE_NAME, OWNER, KEY_SCHEMA, "", null, false));
    assertEquals("The request is missing the Value schema, which is a mandatory field.", exception2.getMessage());
  }

  @Test
  public void testNullOwnerAndAccessPermissions() {
    CreateNewStoreRequest request =
        new CreateNewStoreRequest(CLUSTER_NAME, STORE_NAME, null, KEY_SCHEMA, VALUE_SCHEMA, null, false);

    assertEquals(CLUSTER_NAME, request.getClusterName());
    assertEquals(STORE_NAME, request.getStoreName());
    assertEquals(request.getOwner(), CreateNewStoreRequest.DEFAULT_STORE_OWNER);
    assertEquals(KEY_SCHEMA, request.getKeySchema());
    assertEquals(VALUE_SCHEMA, request.getValueSchema());
    assertNull(request.getAccessPermissions());
    assertFalse(request.isSystemStore());
  }

  @Test
  public void testIsSystemStore() {
    CreateNewStoreRequest request1 =
        new CreateNewStoreRequest(CLUSTER_NAME, STORE_NAME, OWNER, KEY_SCHEMA, VALUE_SCHEMA, null, true);
    assertTrue(request1.isSystemStore());

    CreateNewStoreRequest request2 =
        new CreateNewStoreRequest(CLUSTER_NAME, STORE_NAME, OWNER, KEY_SCHEMA, VALUE_SCHEMA, null, false);
    assertFalse(request2.isSystemStore());
  }
}
