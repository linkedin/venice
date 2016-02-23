package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.RoutingDataRepository;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.log4j.Logger;


/**
 * Get routing data from Helix and convert it to our Venice partition and replica objects.
 * <p>
 * As Helix RoutingTableProvider already cached routing data in local memory. Here we do not need to cache them again .
 * But as there is no way to get partition id list and partition number from RoutingTableProvider directly. this class
 * will cache the partition id list in local memory. And get the partition number from Helix ideal state directly.
 */

public class HelixRoutingDataRepository extends RoutingTableProvider implements RoutingDataRepository {
    private static final Logger logger = Logger.getLogger(HelixRoutingDataRepository.class.getName());
    /**
     * Manager used to communicate with Ehlix.
     */
    private final HelixManager manager;
    /**
     * Builder used to build the data path to access Helix internal data.
     */
    private final PropertyKey.Builder keyBuilder;
    /**
     * Reference of the map which contains relationship between resource and set of partition ids.
     */
    private AtomicReference<Map<String, Set<Integer>>> resourceToPartitionIdsMap = new AtomicReference<>();

    public HelixRoutingDataRepository(HelixManager manager) {
        this.manager = manager;
        keyBuilder = new PropertyKey.Builder(manager.getClusterName());
    }

    public void init() {
        try {
            resourceToPartitionIdsMap.set(new HashMap<>());
            manager.addExternalViewChangeListener(this);
        } catch (Exception e) {
            String errorMessage = "Can not register routing table into Helix";
            logger.error(errorMessage, e);
            throw new VeniceException(errorMessage, e);
        }
    }

    @Override
    public List<Instance> getInstances(@NotNull String resourceName, int partitionId) {
        return getInstances(resourceName, partitionId, HelixState.ONLINE);
    }

    public List<Instance> getInstances(@NotNull String resourceName, int partitionId, HelixState state) {
        logger.debug("Get instances of Resource:" + resourceName + ", Partition:" + partitionId + ", State:" + state);
        List<InstanceConfig> instanceConfigs =
            this.getInstances(resourceName, Partition.getPartitionName(resourceName, partitionId), state.toString());
        if(instanceConfigs.isEmpty()){
            return Collections.emptyList();
        }
        //Query live instances to get additional information live admin port.
        List<PropertyKey> keys = new ArrayList<>(instanceConfigs.size());
        for(InstanceConfig instanceConfig:instanceConfigs){
            String instanceName = instanceConfig.getInstanceName();
            keys.add(keyBuilder.liveInstance(instanceName));
        }
        List<LiveInstance> liveInstances = manager.getHelixDataAccessor().getProperty(keys);
        List<Instance> instances = new ArrayList<>(liveInstances.size());
        instances.addAll(liveInstances.stream()
            .map(liveInstance -> HelixInstanceConverter.convertZNRecordToInstance(liveInstance.getRecord()))
            .collect(Collectors.toList()));
        return instances;
    }

    @Override
    public List<Integer> getPartitionIds(@NotNull String resourceName) {
        Map<String, Set<Integer>> map = resourceToPartitionIdsMap.get();
        if (map.containsKey(resourceName)) {
            return new ArrayList<>(map.get(resourceName));
        } else {
            String errorMessage = "Resource:" + resourceName + " dose not exist";
            logger.warn(errorMessage);
            throw new IllegalArgumentException(errorMessage);
        }
    }

    @Override
    public List<Partition> getPartitions(@NotNull String resourceName) {
        return getPartitions(resourceName, HelixState.ONLINE);
    }

    public List<Partition> getPartitions(@NotNull String resourceName, HelixState state) {
        Map<String, Set<Integer>> map = resourceToPartitionIdsMap.get();
        if (map.containsKey(resourceName)) {
            List<Partition> partitions = new ArrayList<>();
            for (int partitionId : map.get(resourceName)) {
                Partition partition =
                    new Partition(partitionId, resourceName, this.getInstances(resourceName, partitionId, state));
                partitions.add(partition);
            }
            return partitions;
        } else {
            String errorMessage = "Resource:" + resourceName + " dose not exist";
            logger.warn(errorMessage);
            throw new IllegalArgumentException(errorMessage);
        }
    }

    @Override
    public int getNumberOfPartitions(@NotNull String resourceName) {
        // Number of partition should be get from ideal state configuration instead of getting from external view.
        // Because if there is not participant in some partition, parition number in external view will be different from ideal state.
        // But here the semantic should be give me the number of partition assigned when creating resource.
        IdealState idealState = manager.getHelixDataAccessor().getProperty(keyBuilder.idealStates(resourceName));
        if (idealState == null) {
            String errorMessage = "Resource:" + resourceName + " dose not exist";
            logger.warn(errorMessage);
            throw new IllegalArgumentException(errorMessage);
        }
        return idealState.getNumPartitions();
    }

    @Override
    public void onExternalViewChange(List<ExternalView> externalViewList, NotificationContext changeContext) {
        super.onExternalViewChange(externalViewList, changeContext);
        Map<String, Set<Integer>> newResrouceToPartitionIdsMap = new HashMap<>();
        for (ExternalView externalView : externalViewList) {
            HashSet<Integer> partitionIds = new HashSet<>();
            for (String partitionName : externalView.getPartitionSet()) {
                partitionIds.add(Partition.getPartitionIdFromName(partitionName));
            }
            newResrouceToPartitionIdsMap.put(externalView.getResourceName(), partitionIds);
        }
        resourceToPartitionIdsMap.set(newResrouceToPartitionIdsMap);
        logger.info("External view is changed.");
    }
}
