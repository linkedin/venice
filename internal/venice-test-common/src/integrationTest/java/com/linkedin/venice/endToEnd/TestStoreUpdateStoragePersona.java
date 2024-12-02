package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.persona.StoragePersona;
import com.linkedin.venice.utils.TestStoragePersonaUtils;
import com.linkedin.venice.utils.Utils;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestStoreUpdateStoragePersona {
  // Ideally this should work with a single region cluster, but today persona only works with a multi region cluster
  private VeniceTwoLayerMultiRegionMultiClusterWrapper venice;
  private ControllerClient controllerClient;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    venice = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(1)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(1)
            .numberOfRouters(1)
            .replicationFactor(2)
            .sslToStorageNodes(false)
            .sslToKafka(false)
            .build());
    controllerClient = new ControllerClient(venice.getClusterNames()[0], venice.getControllerConnectString());
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(controllerClient);
    Utils.closeQuietlyWithErrorLogged(venice);
  }

  private StoragePersona addPersonaToRepoAndWait(long quota, Optional<Set<String>> storeNames) {
    StoragePersona persona = TestStoragePersonaUtils.createDefaultPersona();
    if (storeNames.isPresent()) {
      persona.getStoresToEnforce().addAll(storeNames.get());
    }
    persona.setQuotaNumber(quota);
    ControllerResponse response = controllerClient
        .createStoragePersona(persona.getName(), quota, persona.getStoresToEnforce(), persona.getOwners());
    if (response.isError())
      throw new VeniceException(response.getError());
    TestUtils.waitForNonDeterministicAssertion(
        60,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(controllerClient.getStoragePersona(persona.getName()).getStoragePersona(), persona));
    return persona;
  }

  private Store setUpTestStoreAndAddToRepo(long quota) {
    Store testStore = TestUtils.createTestStore(Utils.getUniqueString("testStore"), "testStoreOwner", 100);
    testStore.setStorageQuotaInByte(quota);
    controllerClient
        .createNewStore(testStore.getName(), testStore.getOwner(), STRING_SCHEMA.toString(), STRING_SCHEMA.toString());
    controllerClient.updateStore(testStore.getName(), new UpdateStoreQueryParams().setStorageQuotaInByte(quota));
    return testStore;
  }

  @Test
  void testUpdateStoreNewPersonaSuccess() {
    long quota = 100;
    StoragePersona persona = addPersonaToRepoAndWait(quota, Optional.empty());
    Store store = setUpTestStoreAndAddToRepo(quota);
    controllerClient.updateStore(store.getName(), new UpdateStoreQueryParams().setStoragePersona(persona.getName()));
    persona.getStoresToEnforce().add(store.getName());
    TestUtils.waitForNonDeterministicAssertion(
        60,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(controllerClient.getStoragePersona(persona.getName()).getStoragePersona(), persona));
  }

  @Test
  void testUpdateStoreQuotaFailed() {
    long quota = 100;
    Store store = setUpTestStoreAndAddToRepo(quota);
    Set<String> storeNames = new HashSet<>();
    storeNames.add(store.getName());
    StoragePersona persona = addPersonaToRepoAndWait(quota, Optional.of(storeNames));
    ControllerResponse response =
        controllerClient.updateStore(store.getName(), new UpdateStoreQueryParams().setStorageQuotaInByte(quota * 2));
    Assert.assertTrue(response.isError());
    /** Make sure the update failed, nothing was updated */
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
      Assert.assertEquals(
          controllerClient.getStore(store.getName()).getStore().getStorageQuotaInByte(),
          store.getStorageQuotaInByte());
      Assert.assertEquals(controllerClient.getStoragePersona(persona.getName()).getStoragePersona(), persona);
    });
  }

  @Test
  void testUpdateStorePersonaDoesNotExist() {
    Store store = setUpTestStoreAndAddToRepo(100);
    ControllerResponse response =
        controllerClient.updateStore(store.getName(), new UpdateStoreQueryParams().setStoragePersona("failedPersona"));
    Assert.assertTrue(response.isError());
    TestUtils.waitForNonDeterministicAssertion(
        60,
        TimeUnit.SECONDS,
        () -> Assert.assertEquals(
            controllerClient.getStore(store.getName()).getStore().getStorageQuotaInByte(),
            store.getStorageQuotaInByte()));
  }

  @Test
  void testUpdatePersonaFailedAlreadyHasPersona() {
    long quota = 100;

    StoragePersona persona = addPersonaToRepoAndWait(quota, Optional.empty());
    StoragePersona persona2 = addPersonaToRepoAndWait(quota, Optional.empty());
    Set<String> expectedStores = new HashSet<>();
    Store testStore = TestUtils.createTestStore(Utils.getUniqueString("testStore"), "testStoreOwner", 100);
    expectedStores.add(testStore.getName());
    assertCommand(
        controllerClient.createNewStoreWithParameters(
            testStore.getName(),
            testStore.getOwner(),
            STRING_SCHEMA.toString(),
            STRING_SCHEMA.toString(),
            new UpdateStoreQueryParams().setStoragePersona(persona.getName()).setStorageQuotaInByte(quota)));
    ControllerResponse response = controllerClient
        .updateStore(testStore.getName(), new UpdateStoreQueryParams().setStoragePersona(persona2.getName()));
    Assert.assertTrue(response.isError());
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
      Assert.assertEquals(
          controllerClient.getStoragePersona(persona.getName()).getStoragePersona().getStoresToEnforce(),
          expectedStores);
      Assert.assertEquals(
          controllerClient.getStoragePersona(persona2.getName()).getStoragePersona().getStoresToEnforce(),
          new HashSet<>());
    });
  }

  @Test
  void testUpdateStorePersonaQuotaSuccess() {
    long quota = 100;
    StoragePersona persona = addPersonaToRepoAndWait(quota * 2, Optional.empty());
    Store store = setUpTestStoreAndAddToRepo(quota);
    ControllerResponse response = controllerClient.updateStore(
        store.getName(),
        new UpdateStoreQueryParams().setStoragePersona(persona.getName()).setStorageQuotaInByte(quota * 2));
    Assert.assertFalse(response.isError());
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
      Assert.assertEquals(controllerClient.getStore(store.getName()).getStore().getStorageQuotaInByte(), quota * 2);
      Assert.assertEquals(
          controllerClient.getStoragePersona(persona.getName()).getStoragePersona().getStoresToEnforce().size(),
          1);
    });
  }

  @Test
  void testUpdateStorePersonaQuotaFailed() {
    long quota = 100;
    StoragePersona persona = addPersonaToRepoAndWait(quota, Optional.empty());
    Store store = setUpTestStoreAndAddToRepo(quota);
    ControllerResponse response = controllerClient.updateStore(
        store.getName(),
        new UpdateStoreQueryParams().setStoragePersona(persona.getName()).setStorageQuotaInByte(quota * 2));
    Assert.assertTrue(response.isError());
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
      Assert.assertEquals(controllerClient.getStore(store.getName()).getStore().getStorageQuotaInByte(), quota);
      Assert.assertEquals(
          controllerClient.getStoragePersona(persona.getName()).getStoragePersona().getStoresToEnforce().size(),
          0);
    });
  }

  @Test
  void testUpdatePersonaTwoStoresSuccess() {
    long quota = 200;
    Set<String> storeNames = new HashSet<>();
    Store store1 = setUpTestStoreAndAddToRepo(quota / 2);
    storeNames.add(store1.getName());
    StoragePersona persona = addPersonaToRepoAndWait(quota, Optional.of(storeNames));
    Store store2 = setUpTestStoreAndAddToRepo(quota / 2);
    persona.getStoresToEnforce().add(store2.getName());
    ControllerResponse response = controllerClient
        .updateStore(store2.getName(), new UpdateStoreQueryParams().setStoragePersona(persona.getName()));
    Assert.assertFalse(response.isError());
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
      Assert.assertEquals(controllerClient.getStoragePersona(persona.getName()).getStoragePersona(), persona);
    });
  }

  @Test
  void testUpdatePersonaTwoStoresFail() {
    long quota = 200;
    Set<String> storeNames = new HashSet<>();
    Store store1 = setUpTestStoreAndAddToRepo(quota / 2);
    storeNames.add(store1.getName());
    StoragePersona persona = addPersonaToRepoAndWait(quota, Optional.of(storeNames));
    Store store2 = setUpTestStoreAndAddToRepo(quota);
    ControllerResponse response = controllerClient
        .updateStore(store2.getName(), new UpdateStoreQueryParams().setStoragePersona(persona.getName()));
    Assert.assertTrue(response.isError());
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
      Assert.assertEquals(controllerClient.getStoragePersona(persona.getName()).getStoragePersona(), persona);
    });
  }

}
