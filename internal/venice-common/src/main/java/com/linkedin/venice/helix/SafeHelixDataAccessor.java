package com.linkedin.venice.helix;

import java.util.List;
import java.util.Map;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.api.exceptions.HelixMetaDataAccessException;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.MaintenanceSignal;
import org.apache.helix.model.Message;
import org.apache.helix.model.PauseSignal;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.DataUpdater;


/**
 * A wrapper around {@link HelixDataAccessor} which disables the unsafe functionality.
 *
 * Venice should never use a {@link HelixDataAccessor} directly. All uses of this class should
 * always be wrapped in a {@link SafeHelixDataAccessor}.
 *
 * In order to ensure a compile-time guarantee that only safe APIs are called, we explicitly
 * choose to remove the unsafe APIs from the interface. Therefore, we do not actually
 * implement the {@link HelixDataAccessor} interface. These are the removed unsafe APIs:
 *
 * - {@link HelixDataAccessor#getProperty(List, boolean)}
 *     We don't want to allow calling this function, since it can be used unsafely if
 *     throwException=false.
 * - {@link HelixDataAccessor#getChildValues(PropertyKey, boolean)}
 *     We don't want to allow calling this function, since it can be used unsafely if
 *     throwException=false.
 * - {@link HelixDataAccessor#getChildValuesMap(PropertyKey, boolean)}
 *     We don't want to allow calling this function, since it can be used unsafely if
 *     throwException=false.
 * - {@link HelixDataAccessor#getBaseDataAccessor()}
 *     If we want to allow the use of the {@link BaseDataAccessor} returned by this function,
 *     then we would likewise need to wrap it inside of a Safe API, because it provides
 *     unsafe calls such as {@link BaseDataAccessor#get(List, List, int)} and potentially
 *     others.
 *
 * TODO: Review the rest of the functions and determine if more should be culled.
 */
public class SafeHelixDataAccessor {
  private final HelixDataAccessor helixDataAccessor;

  public SafeHelixDataAccessor(HelixDataAccessor helixDataAccessor) {
    this.helixDataAccessor = helixDataAccessor;
  }

  public boolean createStateModelDef(StateModelDefinition stateModelDef) {
    return helixDataAccessor.createStateModelDef(stateModelDef);
  }

  public boolean createControllerMessage(Message message) {
    return helixDataAccessor.createControllerMessage(message);
  }

  public boolean createControllerLeader(LiveInstance leader) {
    return helixDataAccessor.createControllerLeader(leader);
  }

  public boolean createPause(PauseSignal pauseSignal) {
    return helixDataAccessor.createPause(pauseSignal);
  }

  public boolean createMaintenance(MaintenanceSignal maintenanceSignal) {
    return helixDataAccessor.createMaintenance(maintenanceSignal);
  }

  public <T extends HelixProperty> boolean setProperty(PropertyKey key, T value) {
    return helixDataAccessor.setProperty(key, value);
  }

  public <T extends HelixProperty> boolean updateProperty(PropertyKey key, T value) {
    return helixDataAccessor.updateProperty(key, value);
  }

  public <T extends HelixProperty> boolean updateProperty(PropertyKey key, DataUpdater<ZNRecord> updater, T value) {
    return helixDataAccessor.updateProperty(key, updater, value);
  }

  public <T extends HelixProperty> T getProperty(PropertyKey key) {
    return helixDataAccessor.getProperty(key);
  }

  /**
   * N.B.: The behavior of this function is different from that of {@link HelixDataAccessor}, which would
   *       swallow exceptions and return an empty collection.
   *
   * @throws {@link HelixMetaDataAccessException} if the fetch operation failed.
   */
  public <T extends HelixProperty> List<T> getProperty(List<PropertyKey> keys) throws HelixMetaDataAccessException {
    return helixDataAccessor.getProperty(keys, true);
  }

  public boolean removeProperty(PropertyKey key) {
    return helixDataAccessor.removeProperty(key);
  }

  public HelixProperty.Stat getPropertyStat(PropertyKey key) {
    return helixDataAccessor.getPropertyStat(key);
  }

  public List<HelixProperty.Stat> getPropertyStats(List<PropertyKey> keys) {
    return helixDataAccessor.getPropertyStats(keys);
  }

  public List<String> getChildNames(PropertyKey key) {
    return helixDataAccessor.getChildNames(key);
  }

  /**
   * N.B.: The behavior of this function is different from that of {@link HelixDataAccessor}, which would
   *       swallow exceptions and return an empty collection.
   *
   * @throws {@link HelixMetaDataAccessException} if the fetch operation failed.
   */
  public <T extends HelixProperty> List<T> getChildValues(PropertyKey key) throws HelixMetaDataAccessException {
    return helixDataAccessor.getChildValues(key, true);
  }

  /**
   * N.B.: The behavior of this function is different from that of {@link HelixDataAccessor}, which would
   *       swallow exceptions and return an empty collection.
   *
   * @throws {@link HelixMetaDataAccessException} if the fetch operation failed.
   */
  public <T extends HelixProperty> Map<String, T> getChildValuesMap(PropertyKey key)
      throws HelixMetaDataAccessException {
    return helixDataAccessor.getChildValuesMap(key, true);
  }

  public <T extends HelixProperty> boolean[] createChildren(List<PropertyKey> keys, List<T> children) {
    return helixDataAccessor.createChildren(keys, children);
  }

  public <T extends HelixProperty> boolean[] setChildren(List<PropertyKey> keys, List<T> children) {
    return helixDataAccessor.setChildren(keys, children);
  }

  public <T extends HelixProperty> boolean[] updateChildren(
      List<String> paths,
      List<DataUpdater<ZNRecord>> updaters,
      int options) {
    return helixDataAccessor.updateChildren(paths, updaters, options);
  }

  public PropertyKey.Builder keyBuilder() {
    return helixDataAccessor.keyBuilder();
  }
}
