package com.linkedin.venice.job;

/**
 * Status of off-line push in storage node.
 */
//TODO will add more status or refine the definition here in the further.
public enum TaskStatus {
  //Start consuming data from Kafka
  STARTED,
  //The progress of processing the data.
  PRGRESS,
  //Data is read and put into storage engine.
  COMPLETED,
  //Met error when processing the data.
  //TODO will separate it to different types of error later.
  ERROR
}
