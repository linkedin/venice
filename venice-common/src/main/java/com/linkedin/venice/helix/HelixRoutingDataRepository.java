package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.listener.ListenerManager;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.routerapi.ReplicaState;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;
import org.apache.helix.api.exceptions.HelixMetaDataAccessException;
import org.apache.helix.api.listeners.BatchMode;
import org.apache.helix.api.listeners.ControllerChangeListener;
import org.apache.helix.api.listeners.IdealStateChangeListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.api.listeners.RoutingTableChangeListener;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.helix.spectator.RoutingTableSnapshot;
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
@BatchMode
public class HelixRoutingDataRepository implements RoutingDataRepository, ControllerChangeListener, IdealStateChangeListener, RoutingTableChangeListener {
    private static final Logger logger = Logger.getLogger(HelixRoutingDataRepository.class);

    private static final String ONLINE_OFFLINE_VENICE_STATE_FILLER = "N/A";
    /**
     * Manager used to communicate with Helix.
     */
    private final SafeHelixManager manager;
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

    private volatile Map<String, Instance> liveInstancesMap = new HashMap();

    private volatile Map<String, Integer> resourceToIdealPartitionCountMap;

    private long masterControllerChangeTime = -1;

    private RoutingTableProvider routingTableProvider;

    public HelixRoutingDataRepository(SafeHelixManager manager) {
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
            manager.addIdealStateChangeListener(this);
            manager.addControllerListener(this);
            // Use routing table provider to get the notification of the external view change and live instances change.
            routingTableProvider = new RoutingTableProvider(manager.getOriginalManager());
            routingTableProvider.addRoutingTableChangeListener(this, null);
            // Get the current external view and process at first. As the new helix API will not init a event after you add the listener.
            onRoutingTableChange(routingTableProvider.getRoutingTableSnapshot(), null);
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
        manager.removeListener(keyBuilder.idealStates(), this);
        if(routingTableProvider != null) {
            routingTableProvider.removeRoutingTableChangeListener(this);
        }
    }

    /**
     * Get instances from local memory. All of instances are in {@link HelixState#ONLINE} state.
     */
    public List<Instance> getReadyToServeInstances(String kafkaTopic, int partitionId) {
        logger.debug("Get instances of Resource: " + kafkaTopic + ", Partition:" + partitionId);
        Partition partition = resourceAssignment.getPartition(kafkaTopic, partitionId);
        if (partition == null) {
            return Collections.emptyList();
        } else {
            return partition.getReadyToServeInstances();
        }
    }

    public Map<String, List<Instance>> getAllInstances(String kafkaTopic, int partitionId) {
        return getPartitionAssignments(kafkaTopic).getPartition(partitionId).getAllInstances();
    }

    @Override
    public List<ReplicaState> getReplicaStates(String kafkaTopic, int partitionId) {
        Partition partition = resourceAssignment.getPartition(kafkaTopic, partitionId);
        if (partition == null) {
            return Collections.emptyList();
        }
        return partition.getAllInstances().entrySet().stream()
            .flatMap(e -> e.getValue().stream()
                .map(instance -> new ReplicaState(partitionId, instance.getNodeId(), e.getKey(),
                    ONLINE_OFFLINE_VENICE_STATE_FILLER, e.getKey().equals(HelixState.ONLINE_STATE))))
            .collect(Collectors.toList());
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
                    "There is no master controller for this controller or we have not received master changed event from helix.");
        }
        return masterController;
    }

    @Override
    public void subscribeRoutingDataChange(String kafkaTopic, RoutingDataChangedListener listener) {
        listenerManager.subscribe(kafkaTopic, listener);
    }

    @Override
    public void unSubscribeRoutingDataChange(String kafkaTopic, RoutingDataChangedListener listener) {
        listenerManager.unsubscribe(kafkaTopic, listener);
    }

    @Override
    public Map<String, Instance> getLiveInstancesMap() {
        return liveInstancesMap;
    }

    @Override
    public long getMasterControllerChangeTime() {
        return this.masterControllerChangeTime;
    }

    @Override
    public void onControllerChange(NotificationContext changeContext) {
        if (changeContext.getType().equals(NotificationContext.Type.FINALIZE)) {
            //Finalized notification, listener will be removed.
            return;
        }
        logger.info("Got notification type:" + changeContext.getType() + ". Master controller is changed.");
        LiveInstance leader = manager.getHelixDataAccessor().getProperty(keyBuilder.controllerLeader());
        this.masterControllerChangeTime = System.currentTimeMillis();
        if (leader == null) {
            this.masterController = null;
            logger.error("Cluster do not have master controller now!");
        } else {
            this.masterController = createInstanceFromLiveInstance(leader);
            logger.info("New master controller is:" + masterController.getHost() + ":" + masterController.getPort());
        }
    }

    public ResourceAssignment getResourceAssignment() {
        return resourceAssignment;
    }

    @Override
    public boolean doseResourcesExistInIdealState(String resource) {
        PropertyKey key = keyBuilder.idealStates(resource);
        // Try to get the helix property for the given resource, if result is null means the resource does not exist in
        // ideal states.
        if (manager.getHelixDataAccessor().getProperty(key) == null) {
            return false;
        } else {
            return true;
        }
    }

    private Map<String, Instance> convertLiveInstances(Collection<LiveInstance> helixLiveInstances){
        HashMap<String, Instance> instancesMap = new HashMap<>();
        for (LiveInstance helixLiveInstance : helixLiveInstances) {
            Instance instance = createInstanceFromLiveInstance(helixLiveInstance);
            instancesMap.put(instance.getNodeId(), instance);
        }
        return instancesMap;
    }

    private static Instance createInstanceFromLiveInstance(LiveInstance liveInstance) {
        return new Instance(liveInstance.getId(), Utils.parseHostFromHelixNodeIdentifier(liveInstance.getId()),
                Utils.parsePortFromHelixNodeIdentifier(liveInstance.getId()));
    }

    @Override
    public void onIdealStateChange(List<IdealState> idealStates, NotificationContext changeContext)
            throws InterruptedException {
        refreshResourceToIdealPartitionCountMap(idealStates);
    }

    private void refreshResourceToIdealPartitionCountMap(List<IdealState> idealStates) {
        HashMap<String, Integer> partitionCountMap = new HashMap<>();
        for (IdealState idealState : idealStates) {
            // Ideal state could be null, if a resource has already been deleted.
            if(idealState != null) {
                partitionCountMap.put(idealState.getResourceName(), idealState.getNumPartitions());
            }
        }
        this.resourceToIdealPartitionCountMap = Collections.unmodifiableMap(partitionCountMap);
    }

    @Override
    public void onRoutingTableChange(RoutingTableSnapshot routingTableSnapshot, Object context) {
        if(routingTableSnapshot == null || routingTableSnapshot.getExternalViews()==null || routingTableSnapshot.getExternalViews().size()<=0){
            logger.info("Ignore the empty external view.");
            // Update live instances even if there is nonthing in the external view.
            synchronized (liveInstancesMap) {
                liveInstancesMap = convertLiveInstances(routingTableSnapshot.getLiveInstances());
            }
            logger.info("Updated live instances.");
            return;
        }
        Collection<ExternalView> externalViewCollection = routingTableSnapshot.getExternalViews();

        // Create a snapshot to prevent live instances map being changed during this method execution.
        Map<String, Instance> liveInstanceSnapshot = convertLiveInstances(routingTableSnapshot.getLiveInstances());
        //Get number of partitions from Ideal state category in ZK.
        Map<String, Integer> resourceToPartitionCountMapSnapshot = resourceToIdealPartitionCountMap;
        ResourceAssignment newResourceAssignment = new ResourceAssignment();
        Set<String> resourcesInExternalView =
            externalViewCollection.stream().map(ExternalView::getResourceName).collect(Collectors.toSet());
        if (!resourceToPartitionCountMapSnapshot.keySet().containsAll(resourcesInExternalView)) {
            logger.info("Found the inconsistent data between the external view and ideal state of cluster: " + manager
                .getClusterName() + ". Reading the latest ideal state from zk.");

            List<PropertyKey> keys =
                externalViewCollection.stream().map(ev -> keyBuilder.idealStates(ev.getResourceName()))
                    .collect(Collectors.toList());
            try {
                List<IdealState> idealStates = manager.getHelixDataAccessor().getProperty(keys);
                refreshResourceToIdealPartitionCountMap(idealStates);
                resourceToPartitionCountMapSnapshot = resourceToIdealPartitionCountMap;
                logger.info("Ideal state of cluster: " + manager.getClusterName() + " is updated to date from zk");
            } catch (HelixMetaDataAccessException e) {
                logger.error("Failed to update the ideal state of cluster: " + manager.getClusterName()
                    + " because we could not access to zk.", e);
                return;
            }
        }

        for (ExternalView externalView : externalViewCollection) {
            String resourceName = externalView.getResourceName();
            if (!resourceToPartitionCountMapSnapshot.containsKey(resourceName)) {
                logger.warn("Count not find resource: " + resourceName + "in ideal state. Ideal state is up to date,"
                    + " so the resource has been deleted from ideal state. Ignore its external view update.");
                continue;
            }
            PartitionAssignment partitionAssignment =
                new PartitionAssignment(resourceName, resourceToPartitionCountMapSnapshot.get(resourceName));
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
                            logger.warn("Instance:" + instanceName + " unrecognized state:" + instanceStateMap.get(instanceName));
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
            // Update the live instances as well. Helix updates live instances in this routing data changed event.
            this.liveInstancesMap = Collections.unmodifiableMap(liveInstanceSnapshot);
            deletedResourceNames = resourceAssignment.compareAndGetDeletedResources(newResourceAssignment);
            resourceAssignment.refreshAssignment(newResourceAssignment);
            logger.info("Updated resource assignment and live instances.");
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
}
