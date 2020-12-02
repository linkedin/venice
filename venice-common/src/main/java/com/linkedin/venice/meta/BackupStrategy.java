package com.linkedin.venice.meta;

import com.linkedin.venice.exceptions.VeniceException;


/**
 * Enums of the strategies used to backup older store versions in Venice.
 */
public enum BackupStrategy {
    /** Keep numVersionsToPreserve number of backup version.
     */
    KEEP_MIN_VERSIONS,
    /** Delete versions on SN (including kafka and metadata) to preserve only (numVersionsToPreserve-1)
     * backup versions on new push start.
     */
    DELETE_ON_NEW_PUSH_START;
    /** Delete versions on SN (but not kafka and metadata) to preserve only (numVersionsToPreserve-1)
     * backup versions on new push start. So that the deleted versions can be rolled back from Kafka ingestion.
     */
    // KEEP_IN_KAFKA_ONLY,
    /** Keep in user-specified store eg HDD, other DB */
    // KEEP_IN_USER_STORE;

    private static BackupStrategy[] ALL_BACKUP_STRATEGIES = values();

    public static BackupStrategy fromInt(int i) {
        if (i >= ALL_BACKUP_STRATEGIES.length) {
            throw new VeniceException("Invalid BackStrategy ordinal: " + i);
        }
        return ALL_BACKUP_STRATEGIES[i];
    }
}
