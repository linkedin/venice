package com.linkedin.venice.persona;

import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.PathResourceRegistry;
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

  private final ZkBaseDataAccessor<Persona> personaAccessor;
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

  public synchronized void createPersona(String name, long quotaNumber, Set<String> storesToEnforce, Set<String> owners) {
    Persona persona = new StoragePersona(name, quotaNumber, storesToEnforce, owners);
    HelixUtils.create(personaAccessor, getPersonaPath(name), persona);
  }

  public synchronized void updatePersona(Persona persona) {
    HelixUtils.update(personaAccessor, getPersonaPath(persona.getName()), persona);
  }

  public synchronized void deletePersona(Persona persona) { HelixUtils.remove(personaAccessor, getPersonaPath(persona.getName())); }

  public synchronized boolean containsPersona(String name) {
    return personaAccessor.exists(getPersonaPath(name), AccessOption.PERSISTENT);
  }

  public synchronized Persona getPersonaFromZk(String name) {
    return personaAccessor.get(getPersonaPath(name), null, AccessOption.PERSISTENT);
  }

  private String getPersonaPath(String name) {
    return HelixUtils.getHelixClusterZkPath(clusterName) + "/" + PERSONA_SUB_PATH + "/" + STORAGE_PERSONA_SUB_PATH + "/" + name;
  }

}
