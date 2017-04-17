package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.listener.ListenerManager;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;
import org.apache.helix.ControllerChangeListener;
import org.apache.helix.HelixManager;
import org.apache.helix.LiveInstanceChangeListener;
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

public class HelixRoutingDataRepository extends RoutingTableProvider implements RoutingDataRepository, ControllerChangeListener, LiveInstanceChangeListener {
    private static final Logger logger = Logger.getLogger(HelixRoutingDataRepository.class);
    /**
     * Manager used to communicate with Helix.
     */
    private final HelixManager manager;
    /**
     * Builder used to build the data path to access Helix internal data.
     */
    private final PropertyKey.Builder keyBuilder;

    private ResourceAssignment resourceAssignment = new ResourceAssignment();
    /**
     * Master controller of cluster.
     */
    private volatile Instance masterController = null;

    private ListenerManager<RoutingDataChangedListener> listenerManager;

    private volatile Map<String, Instance> liveInstancesMap;

    public HelixRoutingDataRepository(HelixManager manager) {
        this.manager = manager;
        listenerManager = new ListenerManager<>(); //TODO make thread count configurable
        keyBuilder = new PropertyKey.Builder(manager.getClusterName());
    }

    /**
     * This method is used to add listener after HelixManager being connected. Otherwise, it will met error because adding
     * listener before connecting.
     */
    public void refresh() {
        try {
            // After adding the listener, helix will initialize the callback which will get the entire external view
            // and trigger the external view change event. In other words, venice will read the newest external view immediately.
            manager.addLiveInstanceChangeListener(this);
            manager.addExternalViewChangeListener(this);
            manager.addControllerListener(this);
            // TODO subscribe zk state change event after we can get zk client from HelixManager(Should be fixed by Helix team soon)
        } catch (Exception e) {
            String errorMessage = "Cannot register routing table into Helix";
            logger.error(errorMessage, e);
            throw new VeniceException(errorMessage, e);
        }
    }

    public void clear() {
        // removeListener method is a thread safe method, we don't need to lock here again.
        manager.removeListener(keyBuilder.controller(), this);
        manager.removeListener(keyBuilder.externalViews(), this);
        manager.removeListener(keyBuilder.liveInstances(), this);
    }

    /**
     * Get instances from local memory. All of instances are in {@link HelixState#ONLINE} state.
     *
     * @param resourceName
     * @param partitionId
     *
     * @return
     */
    public List<Instance> getReadyToServeInstances(@NotNull String resourceName, int partitionId) {
        logger.debug("Get instances of Resource: " + resourceName + ", Partition:" + partitionId);
        Partition partition = resourceAssignment.getPartition(resourceName, partitionId);
        if (partition == null) {
            return Collections.emptyList();
        } else {
            return partition.getReadyToServeInstances();
        }
    }

    /**
     * Get Partitions from local memory.
     *
     * @param resourceName
     *
     * @return
     */
    public PartitionAssignment getPartitionAssignments(@NotNull String resourceName) {
        return resourceAssignment.getPartitionAssignment(resourceName);
    }

    /**
     * Get number of partition from local memory cache.
     *
     * @param resourceName
     *
     * @return
     */
    public int getNumberOfPartitions(@NotNull String resourceName) {
        return resourceAssignment.getPartitionAssignment(resourceName).getExpectedNumberOfPartitions();
    }

    @Override
    public boolean containsKafkaTopic(String kafkaTopic) {
        return resourceAssignment.containsResource(kafkaTopic);
    }

    @Override
    public Instance getMasterController() {
        if (masterController == null) {
            throw new VeniceException(
                "There are not master controller for this controller or we have not received master changed event from helix.");
        }
        return masterController;
    }

    @Override
    public void subscribeRoutingDataChange(String kafkaTopic, RoutingDataChangedListener listener) {
        listenerManager.subscribe(kafkaTopic,listener);
    }

    @Override
    public void unSubscribeRoutingDataChange(String kafkaTopic, RoutingDataChangedListener listener) {
        listenerManager.unsubscribe(kafkaTopic,listener);
    }

    @Override
    public Map<String, Instance> getLiveInstancesMap() {
        return liveInstancesMap;
    }

    @Override
    public void onExternalViewChange(List<ExternalView> externalViewList, NotificationContext changeContext) {
        super.onExternalViewChange(externalViewList, changeContext);
        if (changeContext.getType().equals(NotificationContext.Type.INIT) && externalViewList.isEmpty()) {
            //Initializing repository and external view is empty. Do nothing for this case,
            return;
        }

        if (changeContext.getType().equals(NotificationContext.Type.FINALIZE)) {
            logger.info("Session has been expired, get the FINALIZE notification.");
            return;
        }
        // Create a snapshot to prevent live instances map being changed during this method execution.
        Map<String, Instance> liveInstanceSnapshot = liveInstancesMap;
        //Get number of partitions from Ideal state category in ZK.
        Set<String> resourcesInExternalView =
            externalViewList.stream().map(ExternalView::getResourceName).collect(Collectors.toSet());
        Map<String, Integer> resourceToPartitionCountMap = getNumberOfPartitionsFromIdealState(resourcesInExternalView);
        ResourceAssignment newResourceAssignment = new ResourceAssignment();
        for (ExternalView externalView : externalViewList) {
            String resourceName = externalView.getResourceName();
            if(!resourceToPartitionCountMap.containsKey(resourceName)) {
                logger.info(resourceName + "has been deleted from ideal state. Ignore its external view update.");
                continue;
            }
            PartitionAssignment partitionAssignment =
                new PartitionAssignment(resourceName, resourceToPartitionCountMap.get(resourceName));
            for (String partitionName : externalView.getPartitionSet()) {
                //Get instance to state map for this partition from local memory.
                Map<String, String> instanceStateMap = externalView.getStateMap(partitionName);
                Map<String, List<Instance>> stateToInstanceMap = new HashMap<>();
                for (String instanceName : instanceStateMap.keySet()) {
                    if (liveInstanceSnapshot.containsKey(instanceName)) {
                        HelixState state;
                        Instance instance = liveInstanceSnapshot.get(instanceName);
                        try {
                            state = HelixState.valueOf(instanceStateMap.get(instanceName));
                        } catch (Exception e) {
                            logger.warn("Instance:" + instanceName + " unrecognized state:" + instanceStateMap.get(
                                instanceName));
                            continue;
                        }
                        if (!stateToInstanceMap.containsKey(state.toString())) {
                            stateToInstanceMap.put(state.toString(), new ArrayList<>());
                        }
                        stateToInstanceMap.get(state.toString()).add(instance);
                    } else {
                        logger.warn("Cannot find instance '" + instanceName + "' in /LIVEINSTANCES");
                    }
                }
                int partitionId = HelixUtils.getPartitionId(partitionName);
                partitionAssignment.addPartition(new Partition(partitionId, stateToInstanceMap));
            }
            newResourceAssignment.setPartitionAssignment(resourceName, partitionAssignment);
        }
        Set<String> deletedResourceNames;
        synchronized (resourceAssignment) {
            deletedResourceNames = resourceAssignment.compareAndGetDeletedResources(newResourceAssignment);
            resourceAssignment.refreshAssignment(newResourceAssignment);
        }
        logger.info("External view is changed.");
        // Start sending notification to listeners. As we can not get the changed data only from Helix, so we just notify all listeners.
        // And assume that the listener would compare and decide how to handle this event.
        for (String kafkaTopic : resourceAssignment.getAssignedResources()) {
            PartitionAssignment partitionAssignment = resourceAssignment.getPartitionAssignment(kafkaTopic);
            listenerManager.trigger(kafkaTopic, new Function<RoutingDataChangedListener, Void>() {
                @Override
                public Void apply(RoutingDataChangedListener listener) {
                    listener.onRoutingDataChanged(partitionAssignment);
                    return null;
                }
            });
        }
        //Notify events to the listeners which listen on deleted resources.
        for (String kafkaTopic : deletedResourceNames) {
            listenerManager.trigger(kafkaTopic, new Function<RoutingDataChangedListener, Void>() {
                @Override
                public Void apply(RoutingDataChangedListener listener) {
                    listener.onRoutingDataDeleted(kafkaTopic);
                    return null;
                }
            });
        }
    }

    private Map<String, Integer> getNumberOfPartitionsFromIdealState(Set<String> newResourceNames) {
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


    @Override
    public void onControllerChange(NotificationContext changeContext) {
        if (changeContext.getType().equals(NotificationContext.Type.FINALIZE)) {
            //Finalized notification, listener will be removed.
            return;
        }
        logger.info("Got notification type:" + changeContext.getType() +". Master controller is changed.");
        LiveInstance leader = manager.getHelixDataAccessor().getProperty(keyBuilder.controllerLeader());
        if (leader == null) {
            this.masterController = null;
            logger.info("Cluster do not have master controller now!");
        } else {
            this.masterController = createInstanceFromLiveInstance(leader);
            logger.info("Controller is:" + masterController.getHost() + ":" + masterController.getPort());
        }
    }

    public ResourceAssignment getResourceAssignment() {
        return resourceAssignment;
    }

    @Override
    public void onLiveInstanceChange(List<LiveInstance> helixLiveInstances, NotificationContext changeContext) {
        HashMap<String, Instance> instancesMap = new HashMap<>();
        for (LiveInstance helixLiveInstance : helixLiveInstances) {
            Instance instance = createInstanceFromLiveInstance(helixLiveInstance);
            instancesMap.put(instance.getNodeId(), instance);
        }
        this.liveInstancesMap= Collections.unmodifiableMap(instancesMap);
    }

    private static Instance createInstanceFromLiveInstance(LiveInstance liveInstance) {
        return new Instance(liveInstance.getId(), Utils.parseHostFromHelixNodeIdentifier(liveInstance.getId()),
            Utils.parsePortFromHelixNodeIdentifier(liveInstance.getId()));
    }
}
