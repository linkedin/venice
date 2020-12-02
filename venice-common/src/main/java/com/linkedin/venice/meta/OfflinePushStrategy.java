package com.linkedin.venice.meta;

import com.linkedin.venice.exceptions.VeniceException;


/**
 * Enum of strategies used to decide the when the data is ready to serve in off-line push.
 */
public enum OfflinePushStrategy {
    /*Wait all replica is ready, the version is ready to serve.*/
    WAIT_ALL_REPLICAS,
    /*Wait until N-1 replicas are ready, the version is ready to serve*/
    WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION;

    private static OfflinePushStrategy[] ALL_OFFLINE_PUSH_STRATEGIES = values();

    public static OfflinePushStrategy getOfflinePushStrategyFromOrdinal(int ordinal) {
        if (ordinal >= ALL_OFFLINE_PUSH_STRATEGIES.length) {
            throw new VeniceException("Invalid OfflinePushStrategy ordinal: " + ordinal);
        }
        return ALL_OFFLINE_PUSH_STRATEGIES[ordinal];
    }
}
