package com.linkedin.venice.meta;

public enum QueryAction {
  // STORAGE is a GET request to storage/storename/key on the router or storage/resourcename/partition/key on the storage node
  STORAGE,HEALTH;
}
