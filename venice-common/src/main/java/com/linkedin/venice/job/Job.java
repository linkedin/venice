package com.linkedin.venice.job;

import java.util.concurrent.atomic.AtomicLong;


/**
 * Created by yayan on 3/7/16.
 */
public class Job {

  private final String jobId;

  private final int numberOfPartition;

  private final int replicaFactor;

  private static AtomicLong idGenerator = new AtomicLong(0l);


  public Job(String kafkaTopic,int numberOfPartition,int replicaFactor){
    this.jobId = generateJobId(kafkaTopic);
    this.numberOfPartition=numberOfPartition;
    this.replicaFactor=replicaFactor;
  }
  public String getJobId() {
    return jobId;
  }

  public int getNumberOfPartition() {
    return numberOfPartition;
  }

  public int getReplicaFactor() {
    return replicaFactor;
  }

  //TODO Generate global id for Job. Could find other better solution in the further.
  public static String generateJobId(String kakfaTopic) {
    //If the controller is failed  then in the same second another standby is chosen to be new master, id could be repeatable here.
    return kakfaTopic + "+" + System.currentTimeMillis() / 1000 + idGenerator.incrementAndGet();
  }
}
