---
layout: default
title: Da Vinci Client
parent: Read APIs
grand_parent: User Guides
permalink: /docs/user_guide/read_api/da_vinci_client
---

# Da Vinci Client
This allows you to eagerly load some or all partitions of the dataset and perform queries against the resulting local 
cache. Future updates to the data continue to be streamed in and applied to the local cache.

## Record Transformer
This feature enables applications to transform records before they're stored in the Da Vinci Client
or a custom storage of your choice.
It's capable of handling records that are compressed and/or chunked.

### Usage
To use the record transformer, you will need to implement the 
[DaVinciRecordTransformer](http://venicedb.org/javadoc/com/linkedin/davinci/client/DaVinciRecordTransformer.html) 
abstract class, then pass in a functional interface into 
[setRecordTransformerFunction()](https://venicedb.org/javadoc/com/linkedin/davinci/client/DaVinciConfig.html#setRecordTransformerFunction(com.linkedin.davinci.client.DaVinciRecordTransformer)). 

When a message is being consumed, the 
[DaVinciRecordTransformer](http://venicedb.org/javadoc/com/linkedin/davinci/client/DaVinciRecordTransformer.html) will 
modify the value before it is written to storage.

Here's an example `DaVinciRecordTransformer` implementation:
```
package com.linkedin.davinci.transformer;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.venice.utils.lazy.Lazy;
import org.apache.avro.Schema;


public class TestStringRecordTransformer extends DaVinciRecordTransformer<Integer, String, String> {
  public TestStringRecordTransformer(int storeVersion, boolean storeRecordsInDaVinci) {
    super(storeVersion, storeRecordsInDaVinci);
  }

  public Schema getKeyOutputSchema() {
    return Schema.create(Schema.Type.INT);
  }

  public Schema getValueOutputSchema() {
    return Schema.create(Schema.Type.STRING);
  }

  public String transform(Lazy<Integer> key, Lazy<String> value) {
    return value.get() + "Transformed";
  }

  public void processPut(Lazy<Integer> key, Lazy<String> value) {
    return;
  }
}
```

Here's an example `setRecordTransformerFunction()` implementation:
```
DaVinciConfig config = new DaVinciConfig();
config.setRecordTransformerFunction((storeVersion) -> new StringRecordTransformer(storeVersion, true));
```