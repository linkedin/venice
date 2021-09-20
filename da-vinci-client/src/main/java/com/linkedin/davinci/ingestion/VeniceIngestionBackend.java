package com.linkedin.davinci.ingestion;

import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.helix.LeaderFollowerPartitionStateModel;
import com.linkedin.davinci.notifier.VeniceNotifier;


public interface VeniceIngestionBackend extends IngestionBackendBase {

  void promoteToLeader(VeniceStoreVersionConfig storeConfig, int partition,
      LeaderFollowerPartitionStateModel.LeaderSessionIdChecker leaderSessionIdChecker);

  void demoteToStandby(VeniceStoreVersionConfig storeConfig, int partition,
      LeaderFollowerPartitionStateModel.LeaderSessionIdChecker leaderSessionIdChecker);

  void addOnlineOfflineIngestionNotifier(VeniceNotifier ingestionListener);

  void addLeaderFollowerIngestionNotifier(VeniceNotifier ingestionListener);

  // addPushStatusNotifier adds ingestion listener which reports ingestion status for different push monitors.
  void addPushStatusNotifier(VeniceNotifier pushStatusNotifier);

}
