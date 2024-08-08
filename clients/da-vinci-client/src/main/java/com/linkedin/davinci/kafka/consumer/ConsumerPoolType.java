package com.linkedin.davinci.kafka.consumer;

public enum ConsumerPoolType {
  AA_WC_LEADER_POOL, // For AA/WC leader
  REGULAR_POOL // For other kinds of workload except AA/WC leader
}
