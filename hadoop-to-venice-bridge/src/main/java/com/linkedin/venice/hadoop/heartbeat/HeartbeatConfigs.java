package com.linkedin.venice.hadoop.heartbeat;

import com.linkedin.venice.utils.Utils;
import java.time.Duration;

public class HeartbeatConfigs {

    public static final Utils.ConfigEntity<Boolean> HEARTBEAT_ENABLED_CONFIG = new Utils.ConfigEntity<>(
            "batch.job.heartbeat.enabled",
            false,
            "If the heartbeat feature is enabled"
    );
    public static final Utils.ConfigEntity<Long> HEARTBEAT_INTERVAL_CONFIG = new Utils.ConfigEntity<>(
            "batch.job.heartbeat.interval.ms",
            Duration.ofMinutes(1).toMillis(),
            "Time interval between sending two consecutive heartbeats"
    );
    public static final Utils.ConfigEntity<Long> HEARTBEAT_INITIAL_DELAY_CONFIG = new Utils.ConfigEntity<>(
            "batch.job.heartbeat.initial.delay.ms",
            0L,
            "Delay before sending the first heartbeat"
    );
    public static final Utils.ConfigEntity<String> HEARTBEAT_VENICE_D2_ZK_HOST_CONFIG = new Utils.ConfigEntity<>(
            "heartbeat.venice.d2.zk.host",
            null,
            "D2 Zookeeper host used to discover the Venice cluster with the heartbeat store"
    );
    public static final Utils.ConfigEntity<String> HEARTBEAT_VENICE_D2_SERVICE_NAME_CONFIG = new Utils.ConfigEntity<>(
            "heartbeat.venice.d2.service.name",
            null,
            "D2 service name used to construct a Venice controller client"
    );
    public static final Utils.ConfigEntity<String> HEARTBEAT_STORE_NAME_CONFIG = new Utils.ConfigEntity<>(
            "heartbeat.store.name",
            null,
            "Heartbeat store name"
    );
    private HeartbeatConfigs() {
        // Util class
    }
}
