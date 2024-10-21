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
Steps to use the record transformer:
1. Implement the 
[DaVinciRecordTransformer](http://venicedb.org/javadoc/com/linkedin/davinci/client/DaVinciRecordTransformer.html) 
abstract class.
2. Create an instance of [DaVinciRecordTransformerConfig](http://venicedb.org/javadoc/com/linkedin/davinci/client/DaVinciRecordTransformerConfig.html).
3. Pass the instance of the config into [setRecordTransformerConfig()](https://venicedb.org/javadoc/com/linkedin/davinci/client/DaVinciConfig.html#setRecordTransformerConfig(com.linkedin.davinci.client.DaVinciRecordTransformerConfig)). 

When a message is being consumed, the 
[DaVinciRecordTransformer](http://venicedb.org/javadoc/com/linkedin/davinci/client/DaVinciRecordTransformer.html) will 
modify the value before it is written to storage.

Here's an example `DaVinciRecordTransformer` implementation:
```
package com.linkedin.davinci.transformer;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.venice.utils.lazy.Lazy;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;


public class StringRecordTransformer extends DaVinciRecordTransformer<Integer, String, String> {
  public TestStringRecordTransformer(int storeVersion, boolean storeRecordsInDaVinci) {
    super(storeVersion, storeRecordsInDaVinci);
  }

  public Schema getKeySchema() {
    return Schema.create(Schema.Type.INT);
  }

  public Schema getOutputValueSchema() {
    return Schema.create(Schema.Type.STRING);
  }

  public DaVinciRecordTransformerResult<String> transform(Lazy<Integer> key, Lazy<String> value) {
    Object valueObj = value.get();
    String valueStr;

    if (valueObj instanceof Utf8) {
      valueStr = valueObj.toString();
    } else {
      valueStr = (String) valueObj;
    }

    String transformedValue = valueStr + "Transformed";
    
    /**
     * If you want to skip a specific record or don't want to modify the value,
     * use the single argument constructor for DaVinciRecordTransformerResult and pass in
     * DaVinciRecordTransformerResult.Result.SKIP or DaVinciRecordTransformerResult.Result.UNCHANGED
     */
    return new DaVinciRecordTransformerResult<>(DaVinciRecordTransformerResult.Result.TRANSFORMED, transformedValue);
  }

  public void processPut(Lazy<Integer> key, Lazy<String> value) {
    return;
  }
}

```

Here's an example `DaVinciRecordTransformerConfig` implementation:
```
DaVinciConfig config = new DaVinciConfig();
DaVinciRecordTransformerConfig recordTransformerConfig = new DaVinciRecordTransformerConfig(
    (storeVersion) -> new StringRecordTransformer(storeVersion, true),
    String.class, Schema.create(Schema.Type.STRING));
config.setRecordTransformerFunction((storeVersion) -> new StringRecordTransformer(storeVersion, true));
```