package com.linkedin.venice.helix;

import com.linkedin.venice.controllerapi.UpdateStoragePersonaQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.persona.StoragePersona;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class StoragePersonaRepositoryTest {

  private final static String quotaFailedRegex = "Invalid persona quota: total store quota exceeds persona quota";
  private final static String storesFailedRegex = "Invalid store\\(s\\) provided: not all stores exist within the cluster, "
      + "one store is already managed by a persona, one store is a system store";
  private final static String personaDoesNotExistRegex = "Update failed: persona with name .* does not exist in this cluster";
  private final static String ownersDoesNotExistRegex = "Invalid owner\\(s\\) provided";


  public HelixReadWriteStoreRepository storeRepository;
  private StoragePersonaRepository personaRepository;
  private ZkClient zkClient;
  private ZkServerWrapper zk;
  private final HelixAdapterSerializer adapter = new HelixAdapterSerializer();
  private final String testClusterName = "testClusterName";

  @BeforeMethod
  public void setUp() {
    zk = ServiceFactory.getZkServer();
    zkClient = ZkClientFactory.newZkClient(zk.getAddress());
    storeRepository = new HelixReadWriteStoreRepository(zkClient, adapter, testClusterName,
        Optional.empty(), new ClusterLockManager(testClusterName));
    personaRepository = new StoragePersonaRepository(testClusterName, storeRepository, adapter, zkClient);
  }

  @AfterMethod
  public void cleanUp() {
    zkClient.close();
    zk.close();
  }

  private Store setUpTestStoreAndAddToRepo(long quota) {
    Store testStore = TestUtils.createTestStore(Utils.getUniqueString("testStore"), "testStoreOwner", 100);
    testStore.setStorageQuotaInByte(quota);
    storeRepository.putStore(testStore);
    return testStore;
  }

  private void addPersonaToRepository(StoragePersona persona) {
    personaRepository.addPersona(persona.getName(), persona.getQuotaNumber(),
        persona.getStoresToEnforce(), persona.getOwners());
  }

  private StoragePersona createDefaultPersona() {
    long quota = 100;
    String testPersonaName = Utils.getUniqueString("testPersona");
    Set<String> testStoreNames = new HashSet<>();
    Set<String> testOwnerNames = new HashSet<>();
    testOwnerNames.add("testOwner");
    return new StoragePersona(testPersonaName, quota, testStoreNames, testOwnerNames);
  }

  @Test
  public void testGetPersona() {
    long quota = 100;
    Store testStore = setUpTestStoreAndAddToRepo(quota);
    StoragePersona defaultPersona = createDefaultPersona();

    Assert.assertNull(personaRepository.getPersona(defaultPersona.getName()));
    defaultPersona.getStoresToEnforce().add(testStore.getName());
    addPersonaToRepository(defaultPersona);
    Assert.assertEquals(defaultPersona, personaRepository.getPersona(defaultPersona.getName()));
  }

  @Test
  public void testGetAllPersonas() {
    long storeQuota = 100;
    Store testStore1 = setUpTestStoreAndAddToRepo(storeQuota);
    Store testStore2 = setUpTestStoreAndAddToRepo(storeQuota);
    List<StoragePersona> personas = new ArrayList<>();
    Assert.assertEquals(personaRepository.getAllPersonas(), personas);
    personas.add(createDefaultPersona());
    personas.get(0).getStoresToEnforce().add(testStore1.getName());
    addPersonaToRepository(personas.get(0));
    personas.add(createDefaultPersona());
    personas.get(1).getStoresToEnforce().add(testStore2.getName());
    addPersonaToRepository(personas.get(1));
    Assert.assertEqualsNoOrder(personaRepository.getAllPersonas().toArray(), personas.toArray());
  }

  @Test
  public void testPersonaNameExists() {
    StoragePersona persona1 = createDefaultPersona();
    addPersonaToRepository(persona1);
    Assert.assertTrue(personaRepository.hasPersona(persona1.getName()));
  }

  @Test(expectedExceptions =  {VeniceException.class}, expectedExceptionsMessageRegExp = storesFailedRegex)
  public void testValidatePersonaStoreDoesNotExist() {
    StoragePersona persona1 = createDefaultPersona();
    persona1.getStoresToEnforce().add("test store");
    personaRepository.validatePersona(persona1);
  }

  @Test(expectedExceptions =  {VeniceException.class}, expectedExceptionsMessageRegExp = quotaFailedRegex)
  public void testValidatePersonaStoreQuotaInvalid() {
    long storeQuota = 100;
    Store testStore1 = setUpTestStoreAndAddToRepo(storeQuota);
    Store testStore2 = setUpTestStoreAndAddToRepo(storeQuota);
    StoragePersona persona = createDefaultPersona();
    persona.getStoresToEnforce().add(testStore1.getName());
    persona.getStoresToEnforce().add(testStore2.getName());
    persona.setQuotaNumber(storeQuota);
    personaRepository.validatePersona(persona);
  }

  @Test(expectedExceptions = {VeniceException.class}, expectedExceptionsMessageRegExp = ownersDoesNotExistRegex)
  public void testValidatePersonaNoOwners() {
    StoragePersona persona1 = createDefaultPersona();
    persona1.setOwners(new HashSet<>());
    personaRepository.validatePersona(persona1);
  }

  @Test(expectedExceptions =  {VeniceException.class}, expectedExceptionsMessageRegExp = storesFailedRegex)
  public void testValidateStoreAlreadyContainedByPersona() {
    long totalQuota = 100;
    String storeName = setUpTestStoreAndAddToRepo(totalQuota).getName();
    StoragePersona persona = createDefaultPersona();
    persona.getStoresToEnforce().add(storeName);
    addPersonaToRepository(persona);
    Assert.assertEquals(personaRepository.getPersona(persona.getName()).getStoresToEnforce().size(), 1);
    Assert.assertEquals(personaRepository.getPersonaContainingStore(storeName), persona.getName());

    StoragePersona persona2 = createDefaultPersona();
    persona2.getStoresToEnforce().add(storeName);
    personaRepository.validatePersona(persona2);
  }

  @Test
  public void testAddPersonaThenMutate() {
    long quota = 100;
    Store testStore = setUpTestStoreAndAddToRepo(quota);
    StoragePersona defaultPersona = createDefaultPersona();
    defaultPersona.getStoresToEnforce().add(testStore.getName());
    defaultPersona.setQuotaNumber(quota * 2);
    addPersonaToRepository(defaultPersona);
    Assert.assertEquals(defaultPersona, personaRepository.getPersona(defaultPersona.getName()));
    Assert.assertEquals(personaRepository.getPersona(defaultPersona.getName()).getStoresToEnforce().size(), 1);
    String testStore2 = setUpTestStoreAndAddToRepo(quota).getName();
    defaultPersona.getStoresToEnforce().add(testStore2);
    Assert.assertEquals(personaRepository.getPersona(defaultPersona.getName()).getStoresToEnforce().size(), 1);
    defaultPersona.setQuotaNumber(quota);
    Assert.assertEquals(personaRepository.getPersona(defaultPersona.getName()).getQuotaNumber(), quota * 2);
    defaultPersona.getOwners().add("new owner");
    Assert.assertEquals(personaRepository.getPersona(defaultPersona.getName()).getOwners().size(), 1);
  }

  @Test
  public void testAddPersonaPersistence() {
    long quota = 100;
    Store testStore = setUpTestStoreAndAddToRepo(quota);
    StoragePersona defaultPersona = createDefaultPersona();
    defaultPersona.getStoresToEnforce().add(testStore.getName());
    defaultPersona.setQuotaNumber(quota * 2);
    addPersonaToRepository(defaultPersona);
    Assert.assertEquals(defaultPersona, personaRepository.getPersona(defaultPersona.getName()));
    Assert.assertEquals(personaRepository.getPersona(defaultPersona.getName()).getStoresToEnforce().size(), 1);
    personaRepository = new StoragePersonaRepository(testClusterName, storeRepository, adapter, zkClient);
    Assert.assertEquals(defaultPersona, personaRepository.getPersona(defaultPersona.getName()));
    Assert.assertEquals(personaRepository.getPersona(defaultPersona.getName()).getStoresToEnforce().size(), 1);
    personaRepository.refresh();
    Assert.assertEquals(defaultPersona, personaRepository.getPersona(defaultPersona.getName()));
    Assert.assertEquals(personaRepository.getPersona(defaultPersona.getName()).getStoresToEnforce().size(), 1);
  }

  @Test
  public void testUpdateQuotaSuccess() {
    long totalQuota = 1000;
    StoragePersona persona = createDefaultPersona();
    persona.setQuotaNumber(totalQuota * 2);
    Store store = setUpTestStoreAndAddToRepo(totalQuota);
    persona.getStoresToEnforce().add(store.getName());
    addPersonaToRepository(persona);
    Assert.assertEquals(personaRepository.getPersona(persona.getName()), persona);
    personaRepository.updatePersona(persona.getName(), new UpdateStoragePersonaQueryParams().setQuota(totalQuota));
    persona.setQuotaNumber(totalQuota);
    Assert.assertEquals(personaRepository.getPersona(persona.getName()), persona);
  }

  @Test(expectedExceptions = {VeniceException.class}, expectedExceptionsMessageRegExp = quotaFailedRegex)
  public void testUpdateQuotaFailed() {
    long totalQuota = 1000;
    StoragePersona persona = createDefaultPersona();
    persona.setQuotaNumber(totalQuota * 2);
    persona.getStoresToEnforce().add(setUpTestStoreAndAddToRepo(totalQuota).getName());
    persona.getStoresToEnforce().add(setUpTestStoreAndAddToRepo(totalQuota).getName());
    Assert.assertEquals(persona.getStoresToEnforce().size(), 2);
    addPersonaToRepository(persona);
    Assert.assertEquals(personaRepository.getPersona(persona.getName()), persona);
    personaRepository.validatePersonaUpdate(persona.getName(), new UpdateStoragePersonaQueryParams().setQuota(totalQuota));
  }

  @Test(expectedExceptions = {VeniceException.class}, expectedExceptionsMessageRegExp = personaDoesNotExistRegex)
  public void testUpdateQuotaFailedDoesNotExist() {
    long totalQuota = 1000;
    personaRepository.validatePersonaUpdate("testPersona",
        new UpdateStoragePersonaQueryParams().setQuota(totalQuota * 2));
  }

  @Test(expectedExceptions = {VeniceException.class}, expectedExceptionsMessageRegExp = personaDoesNotExistRegex)
  public void testUpdateStoresFailedDoesNotExist() {
    long totalQuota = 1000;
    Set<String> storeSet = new HashSet<>();
    storeSet.add(setUpTestStoreAndAddToRepo(totalQuota).getName());
    storeSet.add(setUpTestStoreAndAddToRepo(totalQuota).getName());
    personaRepository.validatePersonaUpdate("testPersona",
        new UpdateStoragePersonaQueryParams().setStoresToEnforce(storeSet));
  }

  @Test(expectedExceptions = {VeniceException.class}, expectedExceptionsMessageRegExp = personaDoesNotExistRegex)
  public void testUpdateQuotaAndStoresFailedDoesNotExist() {
    long totalQuota = 1000;
    Set<String> storeSet = new HashSet<>();
    storeSet.add(setUpTestStoreAndAddToRepo(totalQuota).getName());
    storeSet.add(setUpTestStoreAndAddToRepo(totalQuota).getName());
    personaRepository.validatePersonaUpdate("testPersona",
        new UpdateStoragePersonaQueryParams().setStoresToEnforce(storeSet).setQuota(totalQuota * 2));
  }

  @Test
  public void testUpdateStoresSuccess() {
    long totalQuota = 1000;
    StoragePersona persona = createDefaultPersona();
    persona.setQuotaNumber(totalQuota * 3);
    List<String> stores = new ArrayList<>();
    stores.add(setUpTestStoreAndAddToRepo(totalQuota).getName());
    persona.getStoresToEnforce().add(stores.get(0));
    addPersonaToRepository(persona);
    Assert.assertEquals(personaRepository.getPersona(persona.getName()), persona);
    stores.add(setUpTestStoreAndAddToRepo(totalQuota * 2).getName());
    persona.setStoresToEnforce(new HashSet<>(stores));
    Assert.assertEquals(personaRepository.getPersona(persona.getName()).getStoresToEnforce().size(), 1);
    personaRepository.updatePersona(persona.getName(),
        new UpdateStoragePersonaQueryParams().setStoresToEnforce(new HashSet<>(stores)));
    Assert.assertEquals(personaRepository.getPersona(persona.getName()), persona);
    Assert.assertEquals(personaRepository.getPersona(persona.getName()).getStoresToEnforce().size(), 2);
    Assert.assertEquals(personaRepository.getPersonaContainingStore(stores.get(1)), persona.getName());
  }

  @Test(expectedExceptions =  {VeniceException.class}, expectedExceptionsMessageRegExp = storesFailedRegex)
  public void testUpdateStoresFailInvalidStore() {
    long totalQuota = 1000;
    StoragePersona persona = createDefaultPersona();
    persona.setQuotaNumber(totalQuota * 3);
    List<String> stores = new ArrayList<>();
    stores.add(setUpTestStoreAndAddToRepo(totalQuota).getName());
    persona.getStoresToEnforce().add(stores.get(0));
    addPersonaToRepository(persona);
    Assert.assertEquals(personaRepository.getPersona(persona.getName()), persona);
    stores.add("testFailStore");
    personaRepository.validatePersonaUpdate(persona.getName(),
        new UpdateStoragePersonaQueryParams().setStoresToEnforce(new HashSet<>(stores)));
  }

  @Test(expectedExceptions =  {VeniceException.class}, expectedExceptionsMessageRegExp = quotaFailedRegex)
  public void testUpdateStoresFailedInvalidQuota() {
    long totalQuota = 1000;
    StoragePersona persona = createDefaultPersona();
    persona.setQuotaNumber(totalQuota);
    List<String> stores = new ArrayList<>();
    stores.add(setUpTestStoreAndAddToRepo(totalQuota).getName());
    persona.getStoresToEnforce().add(stores.get(0));
    addPersonaToRepository(persona);
    Assert.assertEquals(personaRepository.getPersona(persona.getName()), persona);
    stores.add(setUpTestStoreAndAddToRepo(totalQuota).getName());
    personaRepository.validatePersonaUpdate(persona.getName(),
        new UpdateStoragePersonaQueryParams().setStoresToEnforce(new HashSet<>(stores)));
    persona.setStoresToEnforce(new HashSet<>(stores));
  }

  @Test
  public void testUpdateStoresAndQuotaPersistence() {
    long totalQuota = 1000;
    StoragePersona persona = createDefaultPersona();
    persona.setQuotaNumber(totalQuota * 3);
    List<String> stores = new ArrayList<>();
    stores.add(setUpTestStoreAndAddToRepo(totalQuota).getName());
    persona.getStoresToEnforce().add(stores.get(0));
    addPersonaToRepository(persona);
    Assert.assertEquals(personaRepository.getPersona(persona.getName()), persona);
    stores.add(setUpTestStoreAndAddToRepo(totalQuota * 2).getName());
    persona.setStoresToEnforce(new HashSet<>(stores));
    persona.setQuotaNumber(totalQuota * 4);
    Assert.assertEquals(personaRepository.getPersona(persona.getName()).getStoresToEnforce().size(), 1);
    personaRepository.updatePersona(persona.getName(),
        new UpdateStoragePersonaQueryParams().setStoresToEnforce(new HashSet<>(stores)).setQuota(totalQuota * 4));
    personaRepository = new StoragePersonaRepository(testClusterName, storeRepository, adapter, zkClient);
    Assert.assertEquals(personaRepository.getPersona(persona.getName()), persona);
    Assert.assertEquals(personaRepository.getPersona(persona.getName()).getStoresToEnforce().size(), 2);
    personaRepository.refresh();
    Assert.assertEquals(personaRepository.getPersona(persona.getName()), persona);
    Assert.assertEquals(personaRepository.getPersona(persona.getName()).getStoresToEnforce().size(), 2);

  }

  @Test
  public void testUpdateRemoveStores() {
    long totalQuota = 1000;
    StoragePersona persona = createDefaultPersona();
    persona.setQuotaNumber(totalQuota * 3);
    List<String> stores = new ArrayList<>();
    stores.add(setUpTestStoreAndAddToRepo(totalQuota).getName());
    stores.add(setUpTestStoreAndAddToRepo(totalQuota * 2).getName());
    persona.setStoresToEnforce(new HashSet<>(stores));
    addPersonaToRepository(persona);
    Assert.assertEquals(personaRepository.getPersona(persona.getName()).getStoresToEnforce().size(), 2);
    stores.remove(1);
    persona.setStoresToEnforce(new HashSet<>(stores));
    personaRepository.updatePersona(persona.getName(),
        new UpdateStoragePersonaQueryParams().setStoresToEnforce(new HashSet<>(stores)));
    Assert.assertEquals(personaRepository.getPersona(persona.getName()), persona);
    Assert.assertEquals(personaRepository.getPersona(persona.getName()).getStoresToEnforce().size(), 1);
    Assert.assertEquals(personaRepository.getPersonaContainingStore(stores.get(0)), persona.getName());

  }

  @Test(expectedExceptions =  {VeniceException.class}, expectedExceptionsMessageRegExp = storesFailedRegex)
  public void testUpdateStoresAlreadyContainedByPersona() {
    long totalQuota = 100;
    String storeName = setUpTestStoreAndAddToRepo(totalQuota).getName();
    StoragePersona persona = createDefaultPersona();
    persona.getStoresToEnforce().add(storeName);
    addPersonaToRepository(persona);
    Assert.assertEquals(personaRepository.getPersona(persona.getName()).getStoresToEnforce().size(), 1);
    Assert.assertEquals(personaRepository.getPersonaContainingStore(storeName), persona.getName());

    StoragePersona persona2 = createDefaultPersona();
    addPersonaToRepository(persona2);

    personaRepository.validatePersonaUpdate(persona2.getName(),
        new UpdateStoragePersonaQueryParams().setStoresToEnforce(persona.getStoresToEnforce()));
  }

  @Test
  public void testDeletePersona() {
    long quota = 100;
    Store testStore = setUpTestStoreAndAddToRepo(quota);
    StoragePersona defaultPersona = createDefaultPersona();
    defaultPersona.getStoresToEnforce().add(testStore.getName());
    addPersonaToRepository(defaultPersona);

    Assert.assertEquals(defaultPersona, personaRepository.getPersona(defaultPersona.getName()));
    personaRepository.deletePersona(defaultPersona.getName());
    Assert.assertNull(personaRepository.getPersona(defaultPersona.getName()));
    Assert.assertEquals(personaRepository.getPersonaContainingStore(testStore.getName()), null);
  }

  @Test
  public void testDeletePersonaDoesNotExist() {
    String name = "testPersona";

    Assert.assertNull(personaRepository.getPersona(name));
    personaRepository.deletePersona(name);
    Assert.assertNull(personaRepository.getPersona(name));
  }

  @Test
  public void testDeletePersonaReUseName() {
    long quota = 100;
    String name = "testPersona";
    Store testStore = setUpTestStoreAndAddToRepo(quota);
    StoragePersona defaultPersona = createDefaultPersona();
    defaultPersona.setName(name);
    defaultPersona.getStoresToEnforce().add(testStore.getName());
    addPersonaToRepository(defaultPersona);

    Assert.assertEquals(defaultPersona, personaRepository.getPersona(defaultPersona.getName()));
    personaRepository.deletePersona(defaultPersona.getName());
    Assert.assertNull(personaRepository.getPersona(defaultPersona.getName()));
    Assert.assertEquals(personaRepository.getPersonaContainingStore(testStore.getName()), null);

    defaultPersona = createDefaultPersona();
    defaultPersona.setName(name);
    addPersonaToRepository(defaultPersona);
    Assert.assertEquals(personaRepository.getPersona(defaultPersona.getName()), defaultPersona);
  }


}
