package com.linkedin.venice.meta;

public enum QueryAction {
  // STORAGE is a GET request to storage/storename/key on the router or storage/resourcename/partition/key on the storage node
  // DICTIONARY is a GET request to storage/storename/version on the storage node to fetch compression dictionary for that version
  STORAGE,HEALTH,COMPUTE,DICTIONARY
}
