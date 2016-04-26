package com.linkedin.venice.helix;

/**
 * States of Venice node in Helix.
 */
public enum HelixState {
    ONLINE, OFFLINE, CATCH_UP, DROPPED, ERROR, LEADER, STANDBY;

    //In StateModelInfo and transition annotation, Only constants string is accepted, so transfer enum to String here.
    public static final String ONLINE_STATE = "ONLINE";
    public static final String OFFLINE_STATE = "OFFLINE";
    public static final String CATCH_UP_STATE = "CATCH_UP";
    public static final String DROPPED_STATE = "DROPPED";
    public static final String ERROR_STATE = "ERROR";
    public static final String LEADER_STATE = "LEADER";
    public static final String STANDBY_STATE = "STANDBY";
}
