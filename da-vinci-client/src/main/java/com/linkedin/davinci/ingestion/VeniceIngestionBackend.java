package com.linkedin.davinci.ingestion;

import com.linkedin.davinci.config.VeniceStoreConfig;
import com.linkedin.davinci.helix.LeaderFollowerParticipantModel;
import com.linkedin.davinci.notifier.VeniceNotifier;


public interface VeniceIngestionBackend extends IngestionBackendBase {

  void promoteToLeader(VeniceStoreConfig storeConfig, int partition,
      LeaderFollowerParticipantModel.LeaderSessionIdChecker leaderSessionIdChecker);

  void demoteToStandby(VeniceStoreConfig storeConfig, int partition,
      LeaderFollowerParticipantModel.LeaderSessionIdChecker leaderSessionIdChecker);

  void addOnlineOfflineIngestionNotifier(VeniceNotifier ingestionListener);

  void addLeaderFollowerIngestionNotifier(VeniceNotifier ingestionListener);

  // addPushStatusNotifier adds ingestion listener which reports ingestion status for different push monitors.
  void addPushStatusNotifier(VeniceNotifier pushStatusNotifier);

}
