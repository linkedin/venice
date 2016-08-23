package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.consumer.KafkaConsumerService;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.notifier.HelixNotifier;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.storage.StorageService;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.Utils;
import javax.validation.constraints.NotNull;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.LiveInstanceInfoProvider;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.log4j.Logger;
import java.util.concurrent.CompletableFuture;


/**
 * Venice Participation Service wrapping Helix Participant.
 */
public class HelixParticipationService extends AbstractVeniceService {

  private static final Logger logger = Logger.getLogger(HelixParticipationService.class);

  private static final String STATE_MODEL_REFERENCE_NAME = "PartitionOnlineOfflineModel";

  private final Instance instance;
  private final String clusterName;
  private final String participantName;
  private final String zkAddress;
  private final StateModelFactory stateModelFactory;
  private final KafkaConsumerService consumerService;
  private final boolean enableServerWhitelist;

  private HelixManager manager;

  public HelixParticipationService(@NotNull KafkaConsumerService kafkaConsumerService,
          @NotNull StorageService storageService,
          @NotNull VeniceConfigLoader veniceConfigLoader,
          @NotNull String zkAddress,
          @NotNull String clusterName,
          int port,
          boolean enableServerWhitelist) {
    this.consumerService = kafkaConsumerService;
    this.clusterName = clusterName;
    //The format of instance name must be "$host_$port", otherwise Helix can not get these information correctly.
    this.participantName = Utils.getHelixNodeIdentifier(port);
    this.zkAddress = zkAddress;
    this.enableServerWhitelist = enableServerWhitelist;
    instance = new Instance(participantName,Utils.getHostName(), port);
    stateModelFactory
        = new VeniceStateModelFactory(kafkaConsumerService, storageService, veniceConfigLoader);
  }

  @Override
  public boolean startInner() {
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

    //TODO Venice Listener should not be started, until the HelixService is started.
    asyncStart();

    // The start up process may not be finished yet, because it is continuing asynchronously.
    return false;
  }

  @Override
  public void stopInner() {
    if(manager != null) {
      manager.disconnect();
    }
  }

  /**
   * check RouterServer#asyncStart() for details about asyncStart
   */
  private void asyncStart() {
    CompletableFuture.runAsync(() -> {
      try {
        HelixUtils.connectHelixManager(manager, 30, 1);
      } catch (VeniceException ve) {
        logger.error(ve.getMessage(), ve);
        logger.error("Venice server is about to close");

        //Since helix manager is necessary. We force to exit the program if it is not able to connected.
        System.exit(1);
      }

      // Report start, progress , completed  and error notifications to controller
      HelixNotifier notifier = new HelixNotifier(manager, participantName);
      consumerService.addNotifier(notifier);

      serviceState.set(ServiceState.STARTED);

      logger.info("Successfully started Helix Participation Service");
    });
  }

}
