package com.linkedin.venice.helix;

import com.linkedin.venice.kafka.consumer.KafkaConsumerService;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.notifier.HelixNotifier;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.storage.StorageService;
import com.linkedin.venice.utils.Utils;
import javax.validation.constraints.NotNull;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.LiveInstanceInfoProvider;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.log4j.Logger;


/**
 * Venice Participation Service wrapping Helix Participant.
 */
public class HelixParticipationService extends AbstractVeniceService {

  private static final Logger logger = Logger.getLogger(HelixParticipationService.class);

  private static final String VENICE_PARTICIPANT_SERVICE_NAME = "venice-participant-service";
  private static final String STATE_MODEL_REFERENCE_NAME = "PartitionOnlineOfflineModel";

  private final Instance instance;
  private final String clusterName;
  private final String participantName;
  private final String zkAddress;
  private final StateModelFactory stateModelFactory;
  private final KafkaConsumerService consumerService;

  private HelixManager manager;

  public HelixParticipationService(@NotNull KafkaConsumerService kafkaConsumerService,
          @NotNull StorageService storageService,
          @NotNull VeniceConfigLoader veniceConfigLoader,
          @NotNull String zkAddress,
          @NotNull String clusterName,
          int port) {

    super(VENICE_PARTICIPANT_SERVICE_NAME);
    this.consumerService = kafkaConsumerService;
    this.clusterName = clusterName;
    //The format of instance name must be "$host_$port", otherwise Helix can not get these information correctly.
    this.participantName = Utils.getHelixNodeIdentifier(port);
    this.zkAddress = zkAddress;
    instance = new Instance(participantName,Utils.getHostName(), port);
    stateModelFactory
        = new VeniceStateModelFactory(kafkaConsumerService, storageService, veniceConfigLoader);
  }

  @Override
  public void startInner() {
    logger.info("Attempting to start HelixParticipation service");
    manager = HelixManagerFactory
            .getZKHelixManager(clusterName, this.participantName, InstanceType.PARTICIPANT,
                zkAddress);
    manager.getStateMachineEngine().registerStateModelFactory(STATE_MODEL_REFERENCE_NAME, stateModelFactory);
    //TODO Now Helix instance config only support host and port. After talking to Helix team, they will add
    // customize k-v data support soon. Then we don't need LiveInstanceInfoProvider here. Use the instance config
    // is a better way because it reduce the communication times to Helix. Other wise client need to get  thsi
    // information from ZK in the extra request and response.
    LiveInstanceInfoProvider liveInstanceInfoProvider = () -> {
      // serialize serviceMetadata to ZNRecord
      return HelixInstanceConverter.convertInstanceToZNRecord(instance);
    };
    manager.setLiveInstanceInfoProvider(liveInstanceInfoProvider);

    try {
      manager.connect();
    } catch (Exception e) {
      logger.error("Error connecting to Helix Manager Cluster " + clusterName + " ZooKeeper Address " + zkAddress + " Participant " + participantName, e);
      throw new RuntimeException(e);
    }

    // Report start, progress , completed  and error notifications to controller
    HelixNotifier notifier = new HelixNotifier(manager , participantName);
    consumerService.addNotifier(notifier);

    logger.info(" Successfully started Helix Participation Service");
  }

  @Override
  public void stopInner() {
    if(manager != null) {
      manager.disconnect();
    }
  }
}
