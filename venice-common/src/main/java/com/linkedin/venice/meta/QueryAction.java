package com.linkedin.venice.meta;

public enum QueryAction {
  // READ is a GET request to read/storename/key on the router or read/resourcename/partition/key on the storage node
  READ;
}
