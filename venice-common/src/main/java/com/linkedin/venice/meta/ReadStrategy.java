package com.linkedin.venice.meta;

import com.linkedin.venice.exceptions.VeniceException;


/**
 * Enums of the strategies used to read data from multiple replications in Venice.
 */
public enum ReadStrategy {
    /*Read from any one online replication.*/
    ANY_OF_ONLINE,
    /*Read from two of replication and use the result returned by the faster one.*/
    FASTER_OF_TWO_ONLINE;

    private static ReadStrategy[] ALL_READ_STRATEGIES = values();

    public static ReadStrategy getReadStrategyFromOrdinal(int ordinal) {
        if (ordinal >= ALL_READ_STRATEGIES.length) {
            throw new VeniceException("Invalid ReadStrategy ordinal: " + ordinal);
        }
        return ALL_READ_STRATEGIES[ordinal];
    }
}
