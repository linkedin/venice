package com.linkedin.venice.persona;

import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.PathResourceRegistry;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.zookeeper.impl.client.ZkClient;


/**
 * Zookeeper accessor for {@link StoragePersona} objects, allowing for read/write to persona paths.
 * <p>
 * The data structure on ZK would be: /Persona/StoragePersona/$name, where $name is specified at creation.
 * Nothing in the accessor is cached locally.
 */
public class StoragePersonaAccessor {
  public static final String STORAGE_PERSONA_SUB_PATH = "StoragePersona";
  private static final String PERSONA_SUB_PATH = "Persona";

  private final ZkBaseDataAccessor<StoragePersona> personaAccessor;
  private final ZkClient zkClient;

  final String clusterName;

  public StoragePersonaAccessor(String clusterName, HelixAdapterSerializer adapter, ZkClient zkClient) {
    this.clusterName = clusterName;
    this.zkClient = zkClient;
    this.personaAccessor = new ZkBaseDataAccessor<>(zkClient);
    registerSerializers(adapter);
    this.zkClient.setZkSerializer(adapter);
  }

  /** Registers a {@link StoragePersonaJSONSerializer} to allow the object to be serialized using the Helix ZooKeeper client. */
  private void registerSerializers(HelixAdapterSerializer adapter) {
    String personaPattern = getPersonaPath(PathResourceRegistry.WILDCARD_MATCH_ANY);
    adapter.registerSerializer(personaPattern, new StoragePersonaJSONSerializer());
  }

  /** Note that this method does not return an error if the persona already exists.  It is up to the caller, i.e.
   * {@link com.linkedin.venice.helix.StoragePersonaRepository} to throw an error in this case.  */
  public synchronized void createPersona(
      String name,
      long quotaNumber,
      Set<String> storesToEnforce,
      Set<String> owners) {
    StoragePersona persona = new StoragePersona(name, quotaNumber, storesToEnforce, owners);
    HelixUtils.create(personaAccessor, getPersonaPath(name), persona);
  }

  public synchronized void createPersona(StoragePersona persona) {
    createPersona(persona.getName(), persona.getQuotaNumber(), persona.getStoresToEnforce(), persona.getOwners());
  }

  public synchronized void updatePersona(StoragePersona persona) {
    HelixUtils.update(personaAccessor, getPersonaPath(persona.getName()), persona);
  }

  public synchronized void deletePersona(StoragePersona persona) {
    deletePersona(persona.getName());
  }

  public synchronized void deletePersona(String personaName) {
    HelixUtils.remove(personaAccessor, getPersonaPath(personaName));
  }

  public synchronized boolean containsPersona(String name) {
    return personaAccessor.exists(getPersonaPath(name), AccessOption.PERSISTENT);
  }

  public synchronized StoragePersona getPersonaFromZk(String name) {
    return personaAccessor.get(getPersonaPath(name), null, AccessOption.PERSISTENT);
  }

  public synchronized List<StoragePersona> getAllPersonasFromZk() {
    List<StoragePersona> personas = personaAccessor.getChildren(getPersonaParentPath(), null, AccessOption.PERSISTENT);
    personas.removeIf(Objects::isNull);
    return personas;
  }

  private String getPersonaPath(String name) {
    return getPersonaParentPath() + "/" + name;
  }

  private String getPersonaParentPath() {
    return HelixUtils.getHelixClusterZkPath(clusterName) + "/" + PERSONA_SUB_PATH + "/" + STORAGE_PERSONA_SUB_PATH;
  }

}
