package com.linkedin.venice.helix;

import com.linkedin.venice.kafka.consumer.KafkaConsumerService;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.server.VeniceConfigService;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.Utils;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.LiveInstanceInfoProvider;
import org.apache.helix.ZNRecord;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.log4j.Logger;


/**
 * Venice Participation Service wrapping Helix Participant.
 */
public class HelixParticipationService extends AbstractVeniceService {

  private static final Logger logger = Logger.getLogger(HelixParticipationService.class.getName());

  private static final String VENICE_PARTICIPANT_SERVICE_NAME = "venice-participant-service";
  private static final String STATE_MODEL_REFERENCE_NAME = "PartitionOnlineOfflineModel";

  private final String hostName;
  private final String port;
  private final String clusterName;
  private final String participantName;
  private final String zkAddress;
  private final StateModelFactory stateModelFactory;

  private HelixManager manager;

  public HelixParticipationService(KafkaConsumerService kafkaConsumerService, StoreRepository storeRepository,
      VeniceConfigService veniceConfigService, String zkAddress, String clusterName, String participantName) {

    super(VENICE_PARTICIPANT_SERVICE_NAME);
    this.clusterName = clusterName;
    this.participantName = participantName;
    this.zkAddress = zkAddress;
    port = veniceConfigService.getVeniceServerConfig().getListenerPort();
    hostName = Utils.getHostName();

    stateModelFactory
        = new VeniceStateModelFactory(kafkaConsumerService, storeRepository, veniceConfigService);
  }

  @Override
  public void startInner() {
    manager = HelixManagerFactory
            .getZKHelixManager(clusterName, participantName, InstanceType.PARTICIPANT, zkAddress);
    manager.getStateMachineEngine().registerStateModelFactory(STATE_MODEL_REFERENCE_NAME, stateModelFactory);

    LiveInstanceInfoProvider liveInstanceInfoProvider = () -> {
      // serialize serviceMetadata to ZNRecord
      ZNRecord rec = new ZNRecord(participantName);
      rec.setSimpleField(LiveInstanceProperty.HOST, hostName);
      rec.setSimpleField(LiveInstanceProperty.PORT, port);
      return rec;
    };
    manager.setLiveInstanceInfoProvider(liveInstanceInfoProvider);

    try {
      manager.connect();
    } catch (Exception e) {
      logger.error("Error connecting to Helix Manager Cluster " + clusterName + " ZooKeeper Address " + zkAddress + " Participant " + participantName, e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void stopInner() {
    if(manager != null) {
      manager.disconnect();
    }
  }
}
