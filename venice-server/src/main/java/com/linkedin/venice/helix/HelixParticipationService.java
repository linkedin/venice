package com.linkedin.venice.helix;

import com.linkedin.venice.kafka.consumer.KafkaConsumerService;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.server.VeniceConfigService;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.Utils;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.LiveInstanceInfoProvider;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.log4j.Logger;


/**
 * Venice Participation Service wrapping Helix Participant.
 */
public class HelixParticipationService extends AbstractVeniceService {

  private static final Logger logger = Logger.getLogger(HelixParticipationService.class.getName());

  private static final String VENICE_PARTICIPANT_SERVICE_NAME = "venice-participant-service";
  private static final String STATE_MODEL_REFERENCE_NAME = "PartitionOnlineOfflineModel";

  private final Instance instance;
  private final String clusterName;
  private final String participantName;
  private final String zkAddress;
  private final StateModelFactory stateModelFactory;

  private HelixManager manager;

  public HelixParticipationService(KafkaConsumerService kafkaConsumerService, StoreRepository storeRepository,
      VeniceConfigService veniceConfigService, String zkAddress, String clusterName, String participantName, int
      httpPort, int adminPort) {

    super(VENICE_PARTICIPANT_SERVICE_NAME);
    this.clusterName = clusterName;
    this.participantName = participantName;
    this.zkAddress = zkAddress;
    instance = new Instance(participantName,Utils.getHostName(),adminPort,httpPort);
    stateModelFactory
        = new VeniceStateModelFactory(kafkaConsumerService, storeRepository, veniceConfigService);
  }

  @Override
  public void startInner() {
    logger.info("Attempting to start HelixParticipation service");
    //The format of instance name must be "$host_$port", otherwise Helix can not get these information correctly.
    manager = HelixManagerFactory
            .getZKHelixManager(clusterName, instance.getHost() + "_" + instance.getHttpPort(), InstanceType.PARTICIPANT,
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
    logger.info(" Successfully started Helix Participation Service");
  }

  @Override
  public void stopInner() {
    if(manager != null) {
      manager.disconnect();
    }
  }
}
