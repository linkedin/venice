package com.linkedin.venice.job;

/**
 * Status of off-line push in storage node.
 */
//TODO will add more status or refine the definition here in the further.
public enum JobAndTaskStatus {
  //Job/Task just be created.
  UNKNOW,
  //Job is started and start consuming data from Kafka
  STARTED,
  //The progress of processing the data. Should only be used for Task.
  PROGRESS,
  //For task, data is read and put into storage engine. For Job, all of tasks are completed.
  COMPLETED,
  //Met error when processing the data.
  //TODO will separate it to different types of error later.
  ERROR,
  //Job is terminated and be removed from repository. Should be archived to historic data storage. Only be used for Job
  ARCHIVED
}
