package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.RoutingDataRepository;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import javax.validation.constraints.NotNull;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.log4j.Logger;


/**
 * Get routing data from Helix and convert it to our Venice partition and replica objects.
 * <p>
 * As Helix RoutingTableProvider already cached routing data in local memory. Here we do not need to cache them again .
 * But as there is no way to get partition id list and partition number from RoutingTableProvider directly. this class
 * will cache the partition id list in local memory.
 */

public class HelixRoutingDataRepostiory extends RoutingTableProvider implements RoutingDataRepository {
    private static final Logger logger = Logger.getLogger(HelixRoutingDataRepostiory.class.getName());

    private final HelixManager manager;
    private AtomicReference<Map<String, Set<Integer>>> resourceToPartitionIdsMap = new AtomicReference<>();

    public HelixRoutingDataRepostiory(HelixManager manager) {
        this.manager = manager;
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
        List<Instance> instances = new ArrayList<>(instanceConfigs.size());
        for (InstanceConfig config : instanceConfigs) {
            instances.add(HelixInstanceConverter.convertZNRecordToInstance(config.getRecord()));
        }
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
    public int getPartitionNumber(@NotNull String resourceName) {
        Map<String, Set<Integer>> map = resourceToPartitionIdsMap.get();
        if (map.containsKey(resourceName)) {
            return map.get(resourceName).size();
        } else {
            String errorMessage = "Resource:" + resourceName + " dose not exist";
            logger.warn(errorMessage);
            throw new IllegalArgumentException(errorMessage);
        }
    }

    @Override
    public void onExternalViewChange(List<ExternalView> externalViewList, NotificationContext changeContext) {
        super.onExternalViewChange(externalViewList, changeContext);
        Map<String, Set<Integer>> newResrouceToPartitionIdsMap = new HashMap<>();
        for (ExternalView externalView : externalViewList) {
            HashSet<Integer> partitionIds = new HashSet<>();
            for (String partitionName : externalView.getPartitionSet()) {
                partitionIds.add(Partition.getParitionIdFromName(partitionName));
            }
            newResrouceToPartitionIdsMap.put(externalView.getResourceName(), partitionIds);
        }
        resourceToPartitionIdsMap.set(newResrouceToPartitionIdsMap);
        logger.info("External view is changed.");
    }
}
