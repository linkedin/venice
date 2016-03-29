package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.RoutingDataChangedListener;
import com.linkedin.venice.meta.RoutingDataRepository;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.log4j.Logger;


/**
 * Get routing data from Helix and convert it to our Venice partition and replica objects.
 * <p>
 * Although Helix RoutingTableProvider already cached routing data in local memory. But it only gets data from
 * /$cluster/EXTERNALVIEW and /$cluster/CONFIGS/PARTICIPANTS. Two parts of data are missed: Additional data in
 * /$cluster/LIVEINSTANCES and partition number in /$cluster/IDEALSTATES. So we cached Venice partitions and instances
 * here to include all of them and also convert them from Helix data structure to Venice data structure.
 * <p>
 * As this repository is used by Router, so here only cached the online instance at first. If Venice needs some more
 * instances in other state, could add them in the further.
 */

public class HelixRoutingDataRepository extends RoutingTableProvider implements RoutingDataRepository {
    private static final Logger logger = Logger.getLogger(HelixRoutingDataRepository.class.getName());
    /**
     * Manager used to communicate with Helix.
     */
    private final HelixManager manager;
    /**
     * Builder used to build the data path to access Helix internal data.
     */
    private final PropertyKey.Builder keyBuilder;
    /**
     * Reference of the map which contains relationship between resource and its partitions.
     */
    private AtomicReference<Map<String, Map<Integer, Partition>>> resourceToPartitionMap = new AtomicReference<>();
    /**
     * Map which contains relationship between resource and its number of partitions.
     */
    private Map<String, Integer> resourceToNumberOfPartitionsMap = new HashMap<>();
    /**
     * Lock used to prevent the conflicts when operating resourceToNumberOfPartitionsMap.
     */
    private final Lock lock = new ReentrantLock();

    private Map<String, HashSet<RoutingDataChangedListener>> listenersMap = new HashMap<>();

    public HelixRoutingDataRepository(HelixManager manager) {
        this.manager = manager;
        keyBuilder = new PropertyKey.Builder(manager.getClusterName());
    }

    /**
     * This method is used to add listener after HelixManager being connected. Otherwise, it will met error because adding
     * listener before connecting.
     */
    public void start() { //TODO: close or clear method
        try {
            resourceToPartitionMap.set(new HashMap());
            manager.addExternalViewChangeListener(this);
        } catch (Exception e) {
            String errorMessage = "Cannot register routing table into Helix";
            logger.error(errorMessage, e);
            throw new VeniceException(errorMessage, e);
        }
    }

    public void clear() {
        manager.removeListener(keyBuilder.externalViews(), this);
        resourceToNumberOfPartitionsMap.clear();
        resourceToPartitionMap.set(null);
    }

    /**
     * Get instances from local memory.
     *
     * @param resourceName
     * @param partitionId
     *
     * @return
     */
    public List<Instance> getInstances(@NotNull String resourceName, int partitionId) {
        logger.debug("Get instances of Resource: " + resourceName + ", Partition:" + partitionId);
        Map<String, Map<Integer, Partition>> map = resourceToPartitionMap.get();
        if (map.containsKey(resourceName)) {
            Map<Integer, Partition> partitionsMap = map.get(resourceName);
            if (partitionsMap.containsKey(partitionId)) {
                return Collections.unmodifiableList(partitionsMap.get(partitionId).getInstances());
            } else {
                //Can not find partition by given partitionId.
                return Collections.emptyList();
            }
        } else {
            String errorMessage = "Resource '" + resourceName + "' does not exist";
            logger.warn(errorMessage);
            throw new VeniceException(errorMessage);
        }
    }

    /**
     * Get Partitions from local memory.
     *
     * @param resourceName
     *
     * @return
     */
    public Map<Integer, Partition> getPartitions(@NotNull String resourceName) {
        Map<String, Map<Integer, Partition>> map = resourceToPartitionMap.get();
        if (map.containsKey(resourceName)) {
            return Collections.unmodifiableMap(map.get(resourceName));
        } else {
            String errorMessage = "Resource '" + resourceName + "' does not exist";
            logger.warn(errorMessage);
            throw new VeniceException(errorMessage);
        }
    }

    /**
     * Get number of partition from local memory.
     * cache.
     *
     * @param resourceName
     *
     * @return
     */
    public int getNumberOfPartitions(@NotNull String resourceName) {
        lock.lock();
        try {
            if (!resourceToNumberOfPartitionsMap.containsKey(resourceName)) {
                String errorMessage = "Resource '" + resourceName + "' does not exist";
                logger.warn(errorMessage);
                // TODO: Might want to add some (configurable) retries here or higher up the stack. If the Helix spectator is out of sync, this fails...
                throw new VeniceException(errorMessage);
            }
            return resourceToNumberOfPartitionsMap.get(resourceName);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean containsKafkaTopic(String kafkaTopic) {
        lock.lock();
        try {
            return resourceToNumberOfPartitionsMap.containsKey(kafkaTopic);
        }finally {
            lock.unlock();
        }
    }

    @Override
    public void subscribeRoutingDataChange(String kafkaTopic, RoutingDataChangedListener listener) {
        synchronized (listenersMap) {
            HashSet<RoutingDataChangedListener> listeners = listenersMap.get(kafkaTopic);
            if (listeners == null) {
                listeners = new HashSet<>();
                listenersMap.put(kafkaTopic, listeners);
            }
            listeners.add(listener);
        }
    }

    @Override
    public void unSubscribeRoutingDataChange(String kafkaTopic, RoutingDataChangedListener listener) {
        synchronized (listenersMap) {
            HashSet<RoutingDataChangedListener> listeners = listenersMap.get(kafkaTopic);
            if (listeners == null) {
                throw new VeniceException("Can not find listener in topic:" + kafkaTopic);
            }
            listeners.remove(listener);
        }
    }

    @Override
    public void onExternalViewChange(List<ExternalView> externalViewList, NotificationContext changeContext) {
        super.onExternalViewChange(externalViewList, changeContext);
        if (changeContext.getType().equals(NotificationContext.Type.INIT) && externalViewList.isEmpty()) {
            //Initializing repository and external view is empty. Do nothing for this case,
            return;
        }
        Map<String, Map<Integer, Partition>> newResourceToPartitionMap = new HashMap<>();
        // Get live instance information from ZK.
        List<LiveInstance> liveInstances = manager.getHelixDataAccessor().getChildValues(keyBuilder.liveInstances());
        Map<String,LiveInstance> liveInstanceMap = new HashMap<>();
        for(LiveInstance liveInstance:liveInstances){
            liveInstanceMap.put(liveInstance.getId(),liveInstance);
        }

        Set<String> deletedResourceNames = new HashSet<>(resourceToNumberOfPartitionsMap.keySet());
        Set<String> addedResourceNames = new HashSet<>();
        for (ExternalView externalView : externalViewList) {
            if (!resourceToNumberOfPartitionsMap.containsKey(externalView.getResourceName())) {
                addedResourceNames.add(externalView.getResourceName());
            } else {
                //Exists in current local cache, delete it from dletedResourceNames.
                deletedResourceNames.remove(externalView.getResourceName());
            }
            //Match live instances and convert them to Venice Instance.
            Map<Integer, Partition> partitionsMap = new HashMap<>();
            for (String partitionName : externalView.getPartitionSet()) {
                //Get instance to state map for this partition from local memory.
                Map<String,String> instanceStateMap = externalView.getStateMap(partitionName);
                List<Instance> instances = new ArrayList<>();
                for(String instanceName:instanceStateMap.keySet()){
                    if (!instanceStateMap.get(instanceName).equals(HelixState.ONLINE.toString())) {
                        //ignore the instance which is not in ONLINE state.
                        logger.info(instanceName + " is not ONLINE. State:" + instanceStateMap.get(instanceName));
                        continue;
                    }
                    if(liveInstanceMap.containsKey(instanceName)){
                        instances.add(HelixInstanceConverter.convertZNRecordToInstance(liveInstanceMap.get(instanceName).getRecord()));
                    }else{
                        logger.warn("Cannot find instance '" + instanceName + "' in LIVEINSTANCES");
                    }
                }
                int partitionId=Partition.getPartitionIdFromName(partitionName);
                partitionsMap.put(partitionId, new Partition(partitionId, externalView.getResourceName(),instances));
            }
            newResourceToPartitionMap.put(externalView.getResourceName(), partitionsMap);
        }
        resourceToPartitionMap.set(newResourceToPartitionMap);

        //Get number of partitions for new resources from ZK.
        Map<String, Integer> newResourceNamesToNumberOfParitions = getNumberOfParitionsFromIdealState(addedResourceNames);
        lock.lock();
        try {
            //Add new added resources.
            resourceToNumberOfPartitionsMap.putAll(newResourceNamesToNumberOfParitions);
            //Clear cached resourceToNumberOfPartition map
            deletedResourceNames.forEach(resourceToNumberOfPartitionsMap::remove);
        }finally {
            lock.unlock();
        }
        logger.debug("Resources added:" + addedResourceNames.toString());
        logger.debug("Resources deleted:" + deletedResourceNames.toString());
        logger.info("External view is changed.");
        // Start sending notification to listeners. As we can not get the changed data only from Helix, so we just notfiy all the listener.
        // And listener will compare and decide how to handle this event.
        synchronized (listenersMap) {
            Map<String, Map<Integer, Partition>> currentPartitionMap = resourceToPartitionMap.get();
            for (String kafkaTopic : listenersMap.keySet()) {
                for (RoutingDataChangedListener listener : listenersMap.get(kafkaTopic)) {
                    listener.handleRoutingDataChange(kafkaTopic, currentPartitionMap.get(kafkaTopic));
                }
            }
        }
    }

    private Map<String, Integer> getNumberOfParitionsFromIdealState(Set<String> newResourceNames) {
        List<PropertyKey> keys = newResourceNames.stream().map(keyBuilder::idealStates).collect(Collectors.toList());
        // Number of partition should be get from ideal state configuration instead of getting from external view.
        // Because if there is not participant in some partition, partition number in external view will be different from ideal state.
        // But here the semantic should be give me the number of partition assigned when creating resource.
        // As in Venice, the number of partition can not be modified after resource being created. So we do not listen the change of IDEASTATE to sync up.
        List<IdealState> idealStates = manager.getHelixDataAccessor().getProperty(keys);
        Map<String, Integer> newResourceNamesToNumberOfParitions = new HashMap<>();
        for (IdealState idealState : idealStates) {
            if (idealState == null) {
                logger.warn("Some resource is deleted after getting the externalViewChanged event.");
            } else {
                newResourceNamesToNumberOfParitions.put(idealState.getResourceName(), idealState.getNumPartitions());
            }
        }
        return newResourceNamesToNumberOfParitions;
    }

}
