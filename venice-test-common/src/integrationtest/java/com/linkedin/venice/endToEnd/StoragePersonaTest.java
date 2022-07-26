package com.linkedin.venice.endToEnd;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoragePersonaQueryParams;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
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
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.utils.TestPushUtils.*;


public class StoragePersonaTest {

  private final static String quotaFailedRegex = "Invalid persona quota: total store quota exceeds persona quota";
  private final static String storesFailedRegex =
      "Invalid store\\(s\\) provided: not all stores exist within the cluster, "
          + "one store is already managed by a persona, one store is a system store";
  private final static String personaDoesNotExistRegex =
      "Update failed: persona with name .* does not exist in this cluster";
  private final static String ownersDoesNotExistRegex = "Invalid owner\\(s\\) provided";

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
    parentController =
        ServiceFactory.getVeniceParentController(venice.getClusterName(), parentZk.getAddress(), venice.getKafka(),
            new VeniceControllerWrapper[]{venice.getLeaderVeniceController()}, false);
    controllerClient = new ControllerClient(venice.getClusterName(), parentController.getControllerUrl());
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(controllerClient);
    Utils.closeQuietlyWithErrorLogged(parentController);
    Utils.closeQuietlyWithErrorLogged(parentZk);
    Utils.closeQuietlyWithErrorLogged(venice);
  }

  private StoragePersona createDefaultPersona() {
    long quota = 100;
    String testPersonaName = Utils.getUniqueString("testPersona");
    Set<String> testStoreNames = new HashSet<>();
    Set<String> testOwnerNames = new HashSet<>();
    testOwnerNames.add("testOwner");
    return new StoragePersona(testPersonaName, quota, testStoreNames, testOwnerNames);
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
    controllerClient.createStoragePersona(persona.getName(), persona.getQuotaNumber(), persona.getStoresToEnforce(),
        persona.getOwners());
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS,
        () -> Assert.assertEquals(controllerClient.getStoragePersona(persona.getName()).getStoragePersona(), persona));
  }

  @Test
  public void testCreatePersonaNonEmptyStores() {
    StoragePersona persona = createDefaultPersona();
    String testStoreName1 = setUpTestStoreAndAddToRepo(100).getName();
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS,
        () -> Assert.assertNotNull(controllerClient.getStore(testStoreName1)));
    persona.getStoresToEnforce().add(testStoreName1);
    String testStoreName2 = setUpTestStoreAndAddToRepo(200).getName();
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS,
        () -> Assert.assertNotNull(controllerClient.getStore(testStoreName2)));
    persona.getStoresToEnforce().add(testStoreName2);
    persona.setQuotaNumber(300);
    ControllerResponse response =
        controllerClient.createStoragePersona(persona.getName(), persona.getQuotaNumber(), persona.getStoresToEnforce(),
            persona.getOwners());
    Assert.assertFalse(response.isError());
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS,
        () -> Assert.assertEquals(controllerClient.getStoragePersona(persona.getName()).getStoragePersona(), persona));
  }

  @Test
  public void testCreateTwoPersonas() {
    StoragePersona persona = createDefaultPersona();
    ControllerResponse response =
        controllerClient.createStoragePersona(persona.getName(), persona.getQuotaNumber(), persona.getStoresToEnforce(),
            persona.getOwners());
    if (response.isError()) {
      throw new VeniceException(response.getError());
    }
    Assert.assertFalse(response.isError());
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS,
        () -> Assert.assertEquals(controllerClient.getStoragePersona(persona.getName()).getStoragePersona(), persona));
    StoragePersona persona2 = createDefaultPersona();
    persona2.setName("testPersona2");
    controllerClient.createStoragePersona(persona2.getName(), persona2.getQuotaNumber(), persona2.getStoresToEnforce(),
        persona2.getOwners());
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS,
        () -> Assert.assertEquals(controllerClient.getStoragePersona(persona2.getName()).getStoragePersona(),
            persona2));
  }

  @Test(expectedExceptions = {
      VeniceException.class}, expectedExceptionsMessageRegExp = ".*Persona with name .* already exists")
  public void testCreatePersonaNameAlreadyExists() {
    StoragePersona persona = createDefaultPersona();
    ControllerResponse response =
        controllerClient.createStoragePersona(persona.getName(), persona.getQuotaNumber(), persona.getStoresToEnforce(),
            persona.getOwners());
    Assert.assertFalse(response.isError());
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS,
        () -> Assert.assertEquals(controllerClient.getStoragePersona(persona.getName()).getStoragePersona(), persona));

    response = controllerClient.createStoragePersona(persona.getName(), 200, new HashSet<>(), persona.getOwners());
    Assert.assertTrue(response.isError());
    // Check to make sure the old persona wasn't affected
    Assert.assertEquals(controllerClient.getStoragePersona(persona.getName()).getStoragePersona(), persona);
    throw new VeniceException(response.getError());
  }

  @Test(expectedExceptions = {VeniceException.class}, expectedExceptionsMessageRegExp = ".*" + storesFailedRegex)
  public void testCreatePersonaStoreDoesNotExist() {
    StoragePersona persona = createDefaultPersona();
    persona.getStoresToEnforce().add("testStore");
    ControllerResponse response =
        controllerClient.createStoragePersona(persona.getName(), persona.getQuotaNumber(), persona.getStoresToEnforce(),
            persona.getOwners());
    Assert.assertTrue(response.isError());
    Assert.assertNull(controllerClient.getStoragePersona(persona.getName()).getStoragePersona());
    throw new VeniceException(response.getError());
  }

  @Test(expectedExceptions = {VeniceException.class}, expectedExceptionsMessageRegExp = ".*" + quotaFailedRegex)
  public void testCreatePersonaInvalidQuota() {
    StoragePersona persona = createDefaultPersona();
    String testStoreName = setUpTestStoreAndAddToRepo(100).getName();
    persona.getStoresToEnforce().add(testStoreName);
    persona.setQuotaNumber(50);
    ControllerResponse response =
        controllerClient.createStoragePersona(persona.getName(), persona.getQuotaNumber(), persona.getStoresToEnforce(),
            persona.getOwners());
    Assert.assertTrue(response.isError());
    Assert.assertNull(controllerClient.getStoragePersona(persona.getName()).getStoragePersona());
    throw new VeniceException(response.getError());
  }

  @Test(expectedExceptions = {VeniceException.class}, expectedExceptionsMessageRegExp = ".*" + ownersDoesNotExistRegex)
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
    controllerClient.createStoragePersona(persona.getName(), persona.getQuotaNumber(), persona.getStoresToEnforce(),
        persona.getOwners());
    ControllerResponse response =
        controllerClient.createStoragePersona(persona.getName(), 200, new HashSet<>(), persona.getOwners());
    Assert.assertTrue(response.isError());
    StoragePersona persona2 = createDefaultPersona();
    response = controllerClient.createStoragePersona(persona2.getName(), persona2.getQuotaNumber(),
        persona2.getStoresToEnforce(), persona2.getOwners());
    Assert.assertFalse(response.isError());
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS,
        () -> Assert.assertEquals(controllerClient.getStoragePersona(persona2.getName()).getStoragePersona(),
            persona2));
  }

  @Test
  public void testDeletePersona() {
    StoragePersona persona = createDefaultPersona();
    controllerClient.createStoragePersona(persona.getName(), persona.getQuotaNumber(), persona.getStoresToEnforce(),
        persona.getOwners());
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS,
        () -> Assert.assertEquals(controllerClient.getStoragePersona(persona.getName()).getStoragePersona(), persona));
    ControllerResponse response = controllerClient.deleteStoragePersona(persona.getName());
    if (response.isError()) throw new VeniceException(response.getError());
    Assert.assertFalse(response.isError());
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> Assert.assertNull(controllerClient.getStoragePersona(persona.getName()).getStoragePersona()));
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
    controllerClient.createStoragePersona(persona.getName(), persona.getQuotaNumber(), persona.getStoresToEnforce(),
        persona.getOwners());
    controllerClient.deleteStoragePersona(persona.getName());
    StoragePersona persona1 = createDefaultPersona();
    persona1.setName(persona.getName());
    controllerClient.createStoragePersona(persona1.getName(), persona1.getQuotaNumber(), persona1.getStoresToEnforce(), persona1.getOwners());
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> Assert.assertEquals(controllerClient.getStoragePersona(persona1.getName()).getStoragePersona(), persona1));
  }

  @Test
  public void testUpdatePersonaSuccess() {
    long totalQuota = 1000;
    StoragePersona persona = createDefaultPersona();
    persona.setQuotaNumber(totalQuota * 3);
    List<String> stores = new ArrayList<>();
    stores.add(setUpTestStoreAndAddToRepo(totalQuota).getName());
    persona.getStoresToEnforce().add(stores.get(0));
    controllerClient.createStoragePersona(persona.getName(), persona.getQuotaNumber(), persona.getStoresToEnforce(), persona.getOwners());
    stores.add(setUpTestStoreAndAddToRepo(totalQuota * 2).getName());
    persona.setStoresToEnforce(new HashSet<>(stores));
    controllerClient.updateStoragePersona(persona.getName(),
        new UpdateStoragePersonaQueryParams().setStoresToEnforce(new HashSet<>(stores)));
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () ->
      Assert.assertEquals(controllerClient.getStoragePersona(persona.getName()).getStoragePersona(), persona));
  }

  @Test(expectedExceptions = {VeniceException.class}, expectedExceptionsMessageRegExp = ".*" + quotaFailedRegex)
  public void testUpdatePersonaFailedQuota() {
    long totalQuota = 1000;
    StoragePersona persona = createDefaultPersona();
    persona.setQuotaNumber(totalQuota);
    List<String> stores = new ArrayList<>();
    stores.add(setUpTestStoreAndAddToRepo(totalQuota).getName());
    persona.getStoresToEnforce().add(stores.get(0));
    controllerClient.createStoragePersona(persona.getName(), persona.getQuotaNumber(), persona.getStoresToEnforce(), persona.getOwners());
    stores.add(setUpTestStoreAndAddToRepo(totalQuota * 2).getName());
    ControllerResponse response = controllerClient.updateStoragePersona(persona.getName(),
        new UpdateStoragePersonaQueryParams().setStoresToEnforce(new HashSet<>(stores)));
    Assert.assertTrue(response.isError());
    throw(new VeniceException(response.getError()));
  }

  @Test(expectedExceptions = {VeniceException.class}, expectedExceptionsMessageRegExp = ".*" + personaDoesNotExistRegex)
  public void testUpdatePersonaFailedDoesNotExist() {
    long totalQuota = 1000;
    StoragePersona persona = createDefaultPersona();
    persona.setQuotaNumber(totalQuota);
    List<String> stores = new ArrayList<>();
    stores.add(setUpTestStoreAndAddToRepo(totalQuota).getName());
    stores.add(setUpTestStoreAndAddToRepo(totalQuota * 2).getName());
    persona.setStoresToEnforce(new HashSet<>(stores));
    ControllerResponse response = controllerClient.updateStoragePersona(persona.getName(),
        new UpdateStoragePersonaQueryParams().setStoresToEnforce(new HashSet<>(stores)));
    Assert.assertTrue(response.isError());
    throw(new VeniceException(response.getError()));
  }

  @Test
  public void testUpdatePersonaFailedNonBlock() {
    ControllerResponse response = controllerClient.updateStoragePersona("failedPersona",
          new UpdateStoragePersonaQueryParams().setStoresToEnforce(new HashSet<>()));
    Assert.assertTrue(response.isError());
    StoragePersona persona2 = createDefaultPersona();
    response = controllerClient.createStoragePersona(persona2.getName(), persona2.getQuotaNumber(),
        persona2.getStoresToEnforce(), persona2.getOwners());
    Assert.assertFalse(response.isError());
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS,
        () -> Assert.assertEquals(controllerClient.getStoragePersona(persona2.getName()).getStoragePersona(),
            persona2));
  }

}