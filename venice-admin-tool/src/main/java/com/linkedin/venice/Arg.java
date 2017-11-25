package com.linkedin.venice;

import com.linkedin.venice.migration.MigrationPushStrategy;


public enum Arg {

  ACCESS_CONTROL("access-control", "acl", true, "Enable/disable store-level access control"),
  URL("url", "u", true, "Venice url, eg. http://localhost:1689  This can be a router or a controller"),
  CLUSTER("cluster", "c", true, "Name of Venice cluster"),
  STORE("store", "s", true, "Name of Venice store"),
  VERSION("version", "v", true, "Venice store version number"),
  PUSH_ID("push-id", "pid", true, "Push Id"),
  STORE_SIZE("store-size", "ss", true, "Size of the store in bytes, used to calculate partitioning"),
  KEY_SCHEMA("key-schema-file", "ks", true, "Path to text file with key schema"),
  VALUE_SCHEMA("value-schema-file", "vs", true, "Path to text file with value schema"),
  OWNER("owner", "o", true, "Owner email for new store creation"),
  STORAGE_NODE("storage-node", "n", true, "Helix instance ID for a storage node, eg. lva1-app1234_1690"),
  KEY("key", "k", true, "Plain-text key for identifying a record in a store"),
  OFFSET("offset", "of", true, "Kafka offset number"),
  EXECUTION("execution", "e", true, "Execution ID of async admin command"),
  PARTITION_COUNT("partition-count", "pn", true, "number of partitions a store has"),
  READABILITY("readability", "rb", true, "store's readability"),
  WRITEABILITY("writeability", "wb", true, "store's writeability"),
  STORAGE_QUOTA("storage-quota", "sq", true, "maximum capacity a store version could have"),
  READ_QUOTA("read-quota", "rq", true, "quota for read request hit this store. Measurement is capacity unit"),
  HYBRID_REWIND_SECONDS("hybrid-rewind-seconds", "hr", true, "for hybrid stores, how far back to rewind in the nearline stream after a batch push completes"),
  HYBRID_OFFSET_LAG("hybrid-offset-lag", "ho", true, "for hybrid stores, what is the offset lag threshold for the storage nodes' consumption to be considered ONLINE"),
  EXPECTED_ROUTER_COUNT("expected-router-count", "erc", true, "How many routers that a cluster should have."),

  VOLDEMORT_STORE("voldemort-store", "vs", true, "Voldemort store name"),
  MIGRATION_PUSH_STRATEGY("migration-push-strategy", "ps", true, "Migration push strategy, valid values: ["
      + MigrationPushStrategy.getAllEnumString() + "]"),
  VSON_STORE("vson_store", "vson", true, "indicate whether it is Vson store or Avro store"),
  COMPRESSION_STRATEGY("compression-strategy", "cs", true, "strategies used to compress/decompress Record's value"),
  CHUNKING_ENABLED("chunking-enabled", "ce", true, "Enable/Disable value chunking, mostly for large value store support"),
  ROUTER_CACHE_ENABLED("router-cache-enabled", "rce", true, "Enable/Disable cache in Router"),

  FILTER_JSON("filter-json", "f", true, "Comma-delimited list of fields to display from the json output.  Omit to display all fields"),
  FLAT_JSON("flat-json", "fj", false, "Display output as flat json, without pretty-print indentation and line breaks"),
  HELP("help", "h", false, "Show usage");


  private final String argName;
  private final String first;
  private final boolean parameterized;
  private final String helpText;

  Arg(String argName, String first, boolean parameterized, String helpText){
    this.argName = argName;
    this.first = first;
    this.parameterized = parameterized;
    this.helpText = helpText;
  }

  @Override
  public String toString(){
    return argName;
  }

  public String first(){
    return first;
  }

  public String getHelpText(){
    return helpText;
  }

  public boolean isParameterized(){
    return parameterized;
  }
}
