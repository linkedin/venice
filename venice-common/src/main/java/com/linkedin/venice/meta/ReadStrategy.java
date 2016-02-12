package com.linkedin.venice.meta;

/**
 * Enums of the strategies used to read data from multiple replications in Venice.
 */
public enum ReadStrategy {
    /*Read from any one online replication.*/
    ANY_OF_ONLINE,
    /*Read from two of replication and use the result returned by the faster one.*/
    FASTER_OF_TOW_ONLINE
}
