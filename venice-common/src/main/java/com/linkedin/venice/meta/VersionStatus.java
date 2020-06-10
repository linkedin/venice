package com.linkedin.venice.meta;

/**
 * Enums of status of verion.
 */
public enum VersionStatus {
    NOT_CREATED,
    STARTED,
    // Version has been pushed to venice(offline job related to this version has been completed), but is not ready to
    // serve read request because the writes to this store is disabled.
    PUSHED,
    // Version has been pushed to venice and is ready to serve read request.
    ONLINE,
    ERROR;

    /**
     * check if a status can be deleted immediately.
     *
     * @param status
     * @return true if it can be deleted immediately, false otherwise
     */
    public static boolean canDelete(VersionStatus status) {
        return ERROR == status;
    }

    /**
     * For all the status which returns true, last few versions
     * (few count, controlled by config) will be preserved.
     *
     * For a store typically last few online versions should be
     * preserved.
     *
     * @param status
     * @return true if it should be considered, false otherwise
     */
    public static boolean preserveLastFew(VersionStatus status) {
        return ONLINE == status;
    }

    /**
     * Check if the Version has completed the bootstrap. We need to make sure that Kafka topic for uncompleted offline
     * job should NOT be deleted. Otherwise Kafka MM would crash. Attention: For streaming case, even version is ONLINE
     * or PUSHED, it might be not safe to delete kafka topic.
     */
    public static boolean isBootstrapCompleted(VersionStatus status) {
        return status.equals(ONLINE) || status.equals(PUSHED);
    }
}
