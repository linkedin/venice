package com.linkedin.venice.kafka.consumer;

/**
 * Created by athirupa on 2/10/16.
 */
public interface VeniceNotifier {

    public void started(long jobId, String storeName, int partitionId);

    public void completed(long jobId, String storeName, int partitionId, long counter);

    public void progress(long jobId, String storeName, int partitionId, long counter);

    public void close();
}
