package com.linkedin.venice.meta;

/**
 * Enums of status of verion.
 */
public enum VersionStatus {
    STARTED, PUSHED, ONLINE, ERROR;

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
}
