package com.linkedin.venice.helix;

import com.linkedin.venice.kafka.consumer.KafkaConsumerService;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.server.VeniceConfigService;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.Utils;
import org.apache.helix.HelixConnection;
import org.apache.helix.HelixParticipant;
import org.apache.helix.LiveInstanceInfoProvider;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.StateTransitionHandlerFactory;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.manager.zk.ZkHelixConnection;
import org.apache.helix.participant.StateMachineEngine;

/**
 * Venice Participation Service wrapping Helix Participant.
 */
public class HelixParticipationService extends AbstractVeniceService {

  private static final String VENICE_PARTICIPANT_SERVICE_NAME = "venice-participant-service";
  private static final String STATE_MODEL_REFERENCE_NAME = "PartitionOnlineOfflineModel";

  private String veniceNodeHostname;
  private String veniceNodePort;

  private final HelixParticipant helixParticipant;

  public HelixParticipationService(KafkaConsumerService kafkaConsumerService, StoreRepository storeRepository,
      VeniceConfigService veniceConfigService, String zkAddress, String clusterName, String participantName) {

    super(VENICE_PARTICIPANT_SERVICE_NAME);
    HelixConnection connection = new ZkHelixConnection(zkAddress);
    connection.connect();
    ClusterId clusterId = ClusterId.from(clusterName);
    ParticipantId participantId = ParticipantId.from(participantName);
    helixParticipant = connection.createParticipant(clusterId, participantId);
    StateMachineEngine stateMachine = helixParticipant.getStateMachineEngine();

    StateTransitionHandlerFactory transitionHandlerFactory
        = new VenicePartitionStateTransitionHandlerFactory(kafkaConsumerService, storeRepository, veniceConfigService);

    stateMachine.registerStateModelFactory(StateModelDefId.from(STATE_MODEL_REFERENCE_NAME), transitionHandlerFactory);

    veniceNodePort = veniceConfigService.getVeniceServerConfig().getListenerPort();
    veniceNodeHostname = Utils.getHostName();
  }

  @Override
  public void startInner() {
    register();
    helixParticipant.start();
  }

  @Override
  public void stopInner() {
    if(helixParticipant != null) {
      helixParticipant.stop();
      if(helixParticipant.getConnection().isConnected()) {
        helixParticipant.getConnection().disconnect();
      }
    }
  }

  public void register() {
    LiveInstanceInfoProvider liveInstanceInfoProvider = () -> {
      // serialize serviceMetadata to ZNRecord
      ZNRecord rec = new ZNRecord(helixParticipant.getParticipantId().stringify());
      rec.setSimpleField(LiveInstanceProperty.HOST, veniceNodeHostname);
      rec.setSimpleField(LiveInstanceProperty.PORT, veniceNodePort);
      return rec;
    };
    helixParticipant.setLiveInstanceInfoProvider(liveInstanceInfoProvider);
  }

}
