package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.routerapi.ReplicaState;
import com.linkedin.venice.utils.HelixUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.helix.PropertyType;
import org.apache.helix.api.exceptions.HelixMetaDataAccessException;
import org.apache.helix.api.listeners.BatchMode;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.spectator.RoutingTableSnapshot;
import org.apache.log4j.Logger;


/**
 * Extend HelixBaseRoutingRepository to leverage external view data.
 */
@BatchMode
public class HelixExternalViewRepository extends HelixBaseRoutingRepository {
    private static final Logger logger = Logger.getLogger(HelixExternalViewRepository.class);

    private static final String ONLINE_OFFLINE_VENICE_STATE_FILLER = "N/A";

    public HelixExternalViewRepository(SafeHelixManager manager) {
        super(manager);
        dataSource.put(PropertyType.EXTERNALVIEW, Collections.emptyList());
    }

    /**
     * Get instances from local memory. All of instances are in {@link HelixState#ONLINE} state.
     */
    public List<Instance> getReadyToServeInstances(PartitionAssignment partitionAssignment, int partitionId) {
        Partition partition = partitionAssignment.getPartition(partitionId);
        if (partition == null) {
            return Collections.emptyList();
        } else {
            return partition.getReadyToServeInstances();
        }
    }

    public List<ReplicaState> getReplicaStates(String kafkaTopic, int partitionId) {
        Partition partition = resourceAssignment.getPartition(kafkaTopic, partitionId);
        if (partition == null) {
            return Collections.emptyList();
        }
        return partition.getAllInstances()
            .entrySet()
            .stream()
            .flatMap(e -> e.getValue()
                .stream()
                .map(instance -> new ReplicaState(partitionId, instance.getNodeId(), e.getKey(),
                    ONLINE_OFFLINE_VENICE_STATE_FILLER, e.getKey().equals(HelixState.ONLINE_STATE))))
            .collect(Collectors.toList());
    }

    public PartitionAssignment convertExternalViewToPartitionAssignment(ExternalView externalView) {
        PartitionAssignment assignment = new PartitionAssignment(externalView.getResourceName(), externalView.getPartitionSet().size());
        // From the external view we have a partition to instance:state mapping.  We need to invert this mapping
        // to be partition to state:instance.
        for(String partition : externalView.getPartitionSet()) {
            Map<String, List<Instance>> stateToInstanceMap = new HashMap<>();
            Map<String, String> instanceToStateMap = externalView.getStateMap(partition);
            for(String instance : instanceToStateMap.keySet()) {
                String state = instanceToStateMap.get(instance);
                // TODO replace with lambda
                if (!stateToInstanceMap.containsKey(state)) {
                    stateToInstanceMap.put(state, new ArrayList<>());
                }
                stateToInstanceMap.get(state).add(Instance.fromNodeId(instance));
            }
            assignment.addPartition(new Partition(HelixUtils.getPartitionId(partition), stateToInstanceMap));
        }
        return assignment;
    }

    @Override
    public void refreshRoutingDataForResource(String resource) {
        // the resourceName is synonymous with the version kafka topic name.  We use it to read the external view from zk
        ExternalView resourceExternalView = manager.getClusterManagmentTool().getResourceExternalView(manager.getClusterName(), resource);
        if(resourceExternalView == null) {
            // We'll have to assume this resource is deleted and move on
            logger.warn(String.format("Could not refresh routing data for resource %s as no external view was reachable", resource));
            return;
        }
        // TODO: Figure out notification implications of this call.  One concern is between this and the on data change
        // we end up going backwards
        synchronized(resourceAssignment) {
            resourceAssignment.setPartitionAssignment(resource, convertExternalViewToPartitionAssignment(resourceExternalView));
        }
        // Notify listeners of this routing update.
        listenerManager.trigger(resource, listener ->
            listener.onExternalViewChange(resourceAssignment.getPartitionAssignment(resource)));
    }

    protected void onExternalViewDataChange(RoutingTableSnapshot routingTableSnapshot) {
        if (routingTableSnapshot.getExternalViews() == null || routingTableSnapshot.getExternalViews().size() <= 0){
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
                logger.info("Ideal state of cluster: " + manager.getClusterName() + " is updated from zk");
            } catch (HelixMetaDataAccessException e) {
                logger.error("Failed to update the ideal state of cluster: " + manager.getClusterName()
                    + " because we could not access to zk.", e);
                return;
            }
        }

        for (ExternalView externalView : externalViewCollection) {
            String resourceName = externalView.getResourceName();
            if (!resourceToPartitionCountMapSnapshot.containsKey(resourceName)) {
                logger.warn("Could not find resource: " + resourceName + " in ideal state. Ideal state is up to date,"
                    + " so the resource has been deleted from ideal state or could not read from zk. Ignore its external view update.");
                continue;
            }
            PartitionAssignment partitionAssignment =
                new PartitionAssignment(resourceName, resourceToPartitionCountMapSnapshot.get(resourceName));
            for (String partitionName : externalView.getPartitionSet()) {
                //Get instance to state map for this partition from local memory.
                Map<String, String> instanceStateMap = externalView.getStateMap(partitionName);
                Map<String, List<Instance>> stateToInstanceMap = new HashMap<>();
                for (String instanceName : instanceStateMap.keySet()) {
                    Instance instance = liveInstanceSnapshot.get(instanceName);
                    if (null != instance) {
                        HelixState state;
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
            listenerManager.trigger(kafkaTopic, listener -> listener.onExternalViewChange(partitionAssignment));
        }

        //Notify events to the listeners which listen on deleted resources.
        for (String kafkaTopic : deletedResourceNames) {
            listenerManager.trigger(kafkaTopic, listener -> listener.onRoutingDataDeleted(kafkaTopic));
        }
    }

    protected void onCustomizedViewDataChange(RoutingTableSnapshot routingTableSnapshot) {
        throw new VeniceException("The function of onCustomizedViewDataChange is not implemented");
    }
}
