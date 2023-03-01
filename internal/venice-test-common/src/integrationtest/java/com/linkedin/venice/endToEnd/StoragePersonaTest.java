package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.utils.TestStoragePersonaUtils.OWNERS_DOES_NOT_EXIST_REGEX;
import static com.linkedin.venice.utils.TestStoragePersonaUtils.PERSONA_DOES_NOT_EXIST_REGEX;
import static com.linkedin.venice.utils.TestStoragePersonaUtils.QUOTA_FAILED_REGEX;
import static com.linkedin.venice.utils.TestStoragePersonaUtils.STORES_FAILED_REGEX;
import static com.linkedin.venice.utils.TestStoragePersonaUtils.createDefaultPersona;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.MultiStoragePersonaResponse;
import com.linkedin.venice.controllerapi.StoragePersonaResponse;
import com.linkedin.venice.controllerapi.UpdateStoragePersonaQueryParams;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerCreateOptions;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.persona.StoragePersona;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class StoragePersonaTest {
  private VeniceClusterWrapper venice;
  private ZkServerWrapper parentZk;
  private VeniceControllerWrapper parentController;
  private ControllerClient controllerClient;

  /**
   * This cluster is re-used by some tests, in order to speed up the suite. Some other tests require
   * certain specific characteristics which makes it awkward to re-use, though not necessarily impossible.
   * Further reuse of this shared cluster can be attempted later.
   */
  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Properties extraProperties = new Properties();
    venice = ServiceFactory.getVeniceCluster(1, 1, 1, 2, 1000000, false, false, extraProperties);
    parentZk = ServiceFactory.getZkServer();
    parentController = ServiceFactory.getVeniceController(
        new VeniceControllerCreateOptions.Builder(venice.getClusterName(), parentZk, venice.getKafka())
            .childControllers(new VeniceControllerWrapper[] { venice.getLeaderVeniceController() })
            .build());
    controllerClient = new ControllerClient(venice.getClusterName(), parentController.getControllerUrl());
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(controllerClient);
    Utils.closeQuietlyWithErrorLogged(parentController);
    Utils.closeQuietlyWithErrorLogged(parentZk);
    Utils.closeQuietlyWithErrorLogged(venice);
  }

  private Store setUpTestStoreAndAddToRepo(long quota) {
    Store testStore = TestUtils.createTestStore(Utils.getUniqueString("testStore"), "testStoreOwner", 100);
    controllerClient.createNewStore(testStore.getName(), testStore.getOwner(), STRING_SCHEMA, STRING_SCHEMA);
    controllerClient.updateStore(testStore.getName(), new UpdateStoreQueryParams().setStorageQuotaInByte(quota));
    return testStore;
  }

  @Test
  public void testCreatePersona() {
    StoragePersona persona = createDefaultPersona();
    Assert.assertNull(controllerClient.getStoragePersona(persona.getName()).getStoragePersona());
    controllerClient.createStoragePersona(
        persona.getName(),
        persona.getQuotaNumber(),
        persona.getStoresToEnforce(),
        persona.getOwners());
    TestUtils.waitForNonDeterministicAssertion(
        60,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(controllerClient.getStoragePersona(persona.getName()).getStoragePersona(), persona));
  }

  @Test
  public void testCreatePersonaNonEmptyStores() {
    StoragePersona persona = createDefaultPersona();
    String testStoreName1 = setUpTestStoreAndAddToRepo(100).getName();
    TestUtils.waitForNonDeterministicAssertion(
        60,
        TimeUnit.SECONDS,
        () -> Assert.assertNotNull(controllerClient.getStore(testStoreName1)));
    persona.getStoresToEnforce().add(testStoreName1);
    String testStoreName2 = setUpTestStoreAndAddToRepo(200).getName();
    TestUtils.waitForNonDeterministicAssertion(
        60,
        TimeUnit.SECONDS,
        () -> Assert.assertNotNull(controllerClient.getStore(testStoreName2)));
    persona.getStoresToEnforce().add(testStoreName2);
    persona.setQuotaNumber(300);
    ControllerResponse response = controllerClient.createStoragePersona(
        persona.getName(),
        persona.getQuotaNumber(),
        persona.getStoresToEnforce(),
        persona.getOwners());
    Assert.assertFalse(response.isError());
    TestUtils.waitForNonDeterministicAssertion(
        60,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(controllerClient.getStoragePersona(persona.getName()).getStoragePersona(), persona));
  }

  @Test
  public void testCreateTwoPersonas() {
    StoragePersona persona = createDefaultPersona();
    ControllerResponse response = controllerClient.createStoragePersona(
        persona.getName(),
        persona.getQuotaNumber(),
        persona.getStoresToEnforce(),
        persona.getOwners());
    if (response.isError()) {
      throw new VeniceException(response.getError());
    }
    Assert.assertFalse(response.isError());
    TestUtils.waitForNonDeterministicAssertion(
        60,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(controllerClient.getStoragePersona(persona.getName()).getStoragePersona(), persona));
    StoragePersona persona2 = createDefaultPersona();
    persona2.setName("testPersona2");
    controllerClient.createStoragePersona(
        persona2.getName(),
        persona2.getQuotaNumber(),
        persona2.getStoresToEnforce(),
        persona2.getOwners());
    TestUtils.waitForNonDeterministicAssertion(
        60,
        TimeUnit.SECONDS,
        () -> Assert
            .assertEquals(controllerClient.getStoragePersona(persona2.getName()).getStoragePersona(), persona2));
  }

  @Test(expectedExceptions = {
      VeniceException.class }, expectedExceptionsMessageRegExp = ".*Persona with name .* already exists")
  public void testCreatePersonaNameAlreadyExists() {
    StoragePersona persona = createDefaultPersona();
    ControllerResponse response = controllerClient.createStoragePersona(
        persona.getName(),
        persona.getQuotaNumber(),
        persona.getStoresToEnforce(),
        persona.getOwners());
    Assert.assertFalse(response.isError());
    TestUtils.waitForNonDeterministicAssertion(
        60,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(controllerClient.getStoragePersona(persona.getName()).getStoragePersona(), persona));

    response = controllerClient.createStoragePersona(persona.getName(), 200, new HashSet<>(), persona.getOwners());
    Assert.assertTrue(response.isError());
    // Check to make sure the old persona wasn't affected
    Assert.assertEquals(controllerClient.getStoragePersona(persona.getName()).getStoragePersona(), persona);
    throw new VeniceException(response.getError());
  }

  @Test(expectedExceptions = { VeniceException.class }, expectedExceptionsMessageRegExp = ".*" + STORES_FAILED_REGEX)
  public void testCreatePersonaStoreDoesNotExist() {
    StoragePersona persona = createDefaultPersona();
    persona.getStoresToEnforce().add("testStore");
    ControllerResponse response = controllerClient.createStoragePersona(
        persona.getName(),
        persona.getQuotaNumber(),
        persona.getStoresToEnforce(),
        persona.getOwners());
    Assert.assertTrue(response.isError());
    Assert.assertNull(controllerClient.getStoragePersona(persona.getName()).getStoragePersona());
    throw new VeniceException(response.getError());
  }

  @Test(expectedExceptions = { VeniceException.class }, expectedExceptionsMessageRegExp = ".*" + QUOTA_FAILED_REGEX)
  public void testCreatePersonaInvalidQuota() {
    StoragePersona persona = createDefaultPersona();
    String testStoreName = setUpTestStoreAndAddToRepo(100).getName();
    persona.getStoresToEnforce().add(testStoreName);
    persona.setQuotaNumber(50);
    ControllerResponse response = controllerClient.createStoragePersona(
        persona.getName(),
        persona.getQuotaNumber(),
        persona.getStoresToEnforce(),
        persona.getOwners());
    Assert.assertTrue(response.isError());
    Assert.assertNull(controllerClient.getStoragePersona(persona.getName()).getStoragePersona());
    throw new VeniceException(response.getError());
  }

  @Test(expectedExceptions = { VeniceException.class }, expectedExceptionsMessageRegExp = ".*"
      + OWNERS_DOES_NOT_EXIST_REGEX)
  public void testCreatePersonaNoOwners() {
    String personaName = "testPersonaNoOwners";
    ControllerResponse response =
        controllerClient.createStoragePersona(personaName, 200, new HashSet<>(), new HashSet<>());
    Assert.assertTrue(response.isError());
    Assert.assertNull(controllerClient.getStoragePersona(personaName).getStoragePersona());
    throw new VeniceException(response.getError());
  }

  @Test
  public void testCreatePersonaErrorCreatePersona() {
    StoragePersona persona = createDefaultPersona();
    controllerClient.createStoragePersona(
        persona.getName(),
        persona.getQuotaNumber(),
        persona.getStoresToEnforce(),
        persona.getOwners());
    ControllerResponse response =
        controllerClient.createStoragePersona(persona.getName(), 200, new HashSet<>(), persona.getOwners());
    Assert.assertTrue(response.isError());
    StoragePersona persona2 = createDefaultPersona();
    response = controllerClient.createStoragePersona(
        persona2.getName(),
        persona2.getQuotaNumber(),
        persona2.getStoresToEnforce(),
        persona2.getOwners());
    Assert.assertFalse(response.isError());
    TestUtils.waitForNonDeterministicAssertion(
        60,
        TimeUnit.SECONDS,
        () -> Assert
            .assertEquals(controllerClient.getStoragePersona(persona2.getName()).getStoragePersona(), persona2));
  }

  @Test
  public void testDeletePersona() {
    StoragePersona persona = createDefaultPersona();
    controllerClient.createStoragePersona(
        persona.getName(),
        persona.getQuotaNumber(),
        persona.getStoresToEnforce(),
        persona.getOwners());
    TestUtils.waitForNonDeterministicAssertion(
        60,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(controllerClient.getStoragePersona(persona.getName()).getStoragePersona(), persona));
    ControllerResponse response = controllerClient.deleteStoragePersona(persona.getName());
    if (response.isError())
      throw new VeniceException(response.getError());
    Assert.assertFalse(response.isError());
    TestUtils.waitForNonDeterministicAssertion(
        60,
        TimeUnit.SECONDS,
        () -> Assert.assertNull(controllerClient.getStoragePersona(persona.getName()).getStoragePersona()));
  }

  @Test
  public void testDeletePersonaDoesNotExist() {
    String personaName = Utils.getUniqueString("testPersonaName");
    ControllerResponse response = controllerClient.deleteStoragePersona(personaName);
    Assert.assertFalse(response.isError());
    Assert.assertNull(controllerClient.getStoragePersona(personaName).getStoragePersona());
  }

  @Test
  public void testDeletePersonaReUseName() {
    StoragePersona persona = createDefaultPersona();
    controllerClient.createStoragePersona(
        persona.getName(),
        persona.getQuotaNumber(),
        persona.getStoresToEnforce(),
        persona.getOwners());
    controllerClient.deleteStoragePersona(persona.getName());
    StoragePersona persona1 = createDefaultPersona();
    persona1.setName(persona.getName());
    controllerClient.createStoragePersona(
        persona1.getName(),
        persona1.getQuotaNumber(),
        persona1.getStoresToEnforce(),
        persona1.getOwners());
    TestUtils.waitForNonDeterministicAssertion(
        60,
        TimeUnit.SECONDS,
        () -> Assert
            .assertEquals(controllerClient.getStoragePersona(persona1.getName()).getStoragePersona(), persona1));
  }

  @Test
  public void testUpdatePersonaSuccess() {
    long totalQuota = 1000;
    StoragePersona persona = createDefaultPersona();
    persona.setQuotaNumber(totalQuota * 3);
    List<String> stores = new ArrayList<>();
    stores.add(setUpTestStoreAndAddToRepo(totalQuota).getName());
    persona.getStoresToEnforce().add(stores.get(0));
    controllerClient.createStoragePersona(
        persona.getName(),
        persona.getQuotaNumber(),
        persona.getStoresToEnforce(),
        persona.getOwners());
    stores.add(setUpTestStoreAndAddToRepo(totalQuota * 2).getName());
    persona.setStoresToEnforce(new HashSet<>(stores));
    controllerClient.updateStoragePersona(
        persona.getName(),
        new UpdateStoragePersonaQueryParams().setStoresToEnforce(new HashSet<>(stores)));
    TestUtils.waitForNonDeterministicAssertion(
        60,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(controllerClient.getStoragePersona(persona.getName()).getStoragePersona(), persona));
  }

  @Test(expectedExceptions = { VeniceException.class }, expectedExceptionsMessageRegExp = ".*" + QUOTA_FAILED_REGEX)
  public void testUpdatePersonaFailedQuota() {
    long totalQuota = 1000;
    StoragePersona persona = createDefaultPersona();
    persona.setQuotaNumber(totalQuota);
    List<String> stores = new ArrayList<>();
    stores.add(setUpTestStoreAndAddToRepo(totalQuota).getName());
    persona.getStoresToEnforce().add(stores.get(0));
    controllerClient.createStoragePersona(
        persona.getName(),
        persona.getQuotaNumber(),
        persona.getStoresToEnforce(),
        persona.getOwners());
    stores.add(setUpTestStoreAndAddToRepo(totalQuota * 2).getName());
    ControllerResponse response = controllerClient.updateStoragePersona(
        persona.getName(),
        new UpdateStoragePersonaQueryParams().setStoresToEnforce(new HashSet<>(stores)));
    Assert.assertTrue(response.isError());
    throw (new VeniceException(response.getError()));
  }

  @Test(expectedExceptions = { VeniceException.class }, expectedExceptionsMessageRegExp = ".*"
      + PERSONA_DOES_NOT_EXIST_REGEX)
  public void testUpdatePersonaFailedDoesNotExist() {
    long totalQuota = 1000;
    StoragePersona persona = createDefaultPersona();
    persona.setQuotaNumber(totalQuota);
    List<String> stores = new ArrayList<>();
    stores.add(setUpTestStoreAndAddToRepo(totalQuota).getName());
    stores.add(setUpTestStoreAndAddToRepo(totalQuota * 2).getName());
    persona.setStoresToEnforce(new HashSet<>(stores));
    ControllerResponse response = controllerClient.updateStoragePersona(
        persona.getName(),
        new UpdateStoragePersonaQueryParams().setStoresToEnforce(new HashSet<>(stores)));
    Assert.assertTrue(response.isError());
    throw (new VeniceException(response.getError()));
  }

  @Test
  public void testUpdatePersonaFailedNonBlock() {
    ControllerResponse response = controllerClient.updateStoragePersona(
        "failedPersona",
        new UpdateStoragePersonaQueryParams().setStoresToEnforce(new HashSet<>()));
    Assert.assertTrue(response.isError());
    StoragePersona persona2 = createDefaultPersona();
    response = controllerClient.createStoragePersona(
        persona2.getName(),
        persona2.getQuotaNumber(),
        persona2.getStoresToEnforce(),
        persona2.getOwners());
    Assert.assertFalse(response.isError());
    TestUtils.waitForNonDeterministicAssertion(
        60,
        TimeUnit.SECONDS,
        () -> Assert
            .assertEquals(controllerClient.getStoragePersona(persona2.getName()).getStoragePersona(), persona2));
  }

  @Test
  public void testGetPersonaContainingStore() {
    long quota = 1000;
    Store testStore = setUpTestStoreAndAddToRepo(quota);
    StoragePersona persona = createDefaultPersona();
    persona.getStoresToEnforce().add(testStore.getName());
    persona.setQuotaNumber(quota);
    controllerClient.createStoragePersona(
        persona.getName(),
        persona.getQuotaNumber(),
        persona.getStoresToEnforce(),
        persona.getOwners());
    TestUtils.waitForNonDeterministicAssertion(
        60,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(
            controllerClient.getStoragePersonaAssociatedWithStore(testStore.getName()).getStoragePersona(),
            persona));
  }

  @Test
  public void testGetPersonaContainingStorePersonaUpdate() {
    long quota = 1000;
    Store testStore = setUpTestStoreAndAddToRepo(quota);
    StoragePersona persona = createDefaultPersona();
    persona.setQuotaNumber(quota);
    controllerClient.createStoragePersona(
        persona.getName(),
        persona.getQuotaNumber(),
        persona.getStoresToEnforce(),
        persona.getOwners());
    persona.getStoresToEnforce().add(testStore.getName());
    controllerClient.updateStoragePersona(
        persona.getName(),
        new UpdateStoragePersonaQueryParams().setStoresToEnforce(persona.getStoresToEnforce()));
    TestUtils.waitForNonDeterministicAssertion(
        60,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(
            controllerClient.getStoragePersonaAssociatedWithStore(testStore.getName()).getStoragePersona(),
            persona));
  }

  @Test
  public void testGetPersonaContainingStoreDoesNotExist() {
    StoragePersonaResponse response = controllerClient.getStoragePersonaAssociatedWithStore("testStoreDoesNotExist");
    Assert.assertNull(response.getStoragePersona());
    Assert.assertFalse(response.isError());
  }

  @Test
  public void testGetPersonaContainingStoreNoPersona() {
    Store testStore = setUpTestStoreAndAddToRepo(1000);
    StoragePersonaResponse response = controllerClient.getStoragePersonaAssociatedWithStore(testStore.getName());
    Assert.assertNull(response.getStoragePersona());
    Assert.assertFalse(response.isError());
  }

  @Test
  public void testGetAllPersonas() {
    long quota = 200;
    List<StoragePersona> expected = controllerClient.getClusterStoragePersonas().getStoragePersonas();
    int originalSize = expected.size();
    Assert.assertNotNull(expected);
    MultiStoragePersonaResponse response = controllerClient.getClusterStoragePersonas();
    Assert.assertEqualsNoOrder(
        response.getStoragePersonas().toArray(new StoragePersona[0]),
        expected.toArray(new StoragePersona[0]));
    StoragePersona persona = createDefaultPersona();
    controllerClient.createStoragePersona(
        persona.getName(),
        persona.getQuotaNumber(),
        persona.getStoresToEnforce(),
        persona.getOwners());
    expected.add(persona);
    persona = createDefaultPersona();
    persona.getStoresToEnforce().add(setUpTestStoreAndAddToRepo(quota).getName());
    persona.setQuotaNumber(quota);
    expected.add(persona);
    controllerClient.createStoragePersona(
        persona.getName(),
        persona.getQuotaNumber(),
        persona.getStoresToEnforce(),
        persona.getOwners());
    TestUtils.waitForNonDeterministicAssertion(
        60,
        TimeUnit.SECONDS,
        () -> Assert.assertEqualsNoOrder(
            controllerClient.getClusterStoragePersonas().getStoragePersonas().toArray(new StoragePersona[0]),
            expected.toArray(new StoragePersona[0])));
    controllerClient.deleteStoragePersona(expected.get(0).getName());
    expected.remove(0);
    Assert.assertEquals(expected.size(), originalSize + 1);
    TestUtils.waitForNonDeterministicAssertion(
        60,
        TimeUnit.SECONDS,
        () -> Assert.assertEqualsNoOrder(
            controllerClient.getClusterStoragePersonas().getStoragePersonas().toArray(new StoragePersona[0]),
            expected.toArray(new StoragePersona[0])));
  }

}
