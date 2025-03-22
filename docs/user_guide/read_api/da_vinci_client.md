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
or redirected to a custom storage of your choice.
It's capable of handling records that are compressed and/or chunked.

### Example Usage
Steps to use the record transformer:
1. Implement the 
[DaVinciRecordTransformer](http://venicedb.org/javadoc/com/linkedin/davinci/client/DaVinciRecordTransformer.html) 
abstract class.
2. Create an instance of [DaVinciRecordTransformerConfig](http://venicedb.org/javadoc/com/linkedin/davinci/client/DaVinciRecordTransformerConfig.html).
3. Pass the instance of the config into [setRecordTransformerConfig()](https://venicedb.org/javadoc/com/linkedin/davinci/client/DaVinciConfig.html#setRecordTransformerConfig(com.linkedin.davinci.client.DaVinciRecordTransformerConfig)).

Here's an example `DaVinciRecordTransformer` implementation:
```
import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;


public class StringRecordTransformer extends DaVinciRecordTransformer<Integer, String, String> {
  public StringRecordTransformer(
      int storeVersion,
      Schema keySchema,
      Schema inputValueSchema,
      Schema outputValueSchema,
      DaVinciRecordTransformerConfig recordTransformerConfig) {
    super(storeVersion, keySchema, inputValueSchema, outputValueSchema, recordTransformerConfig);
  }

  @Override
  public DaVinciRecordTransformerResult<String> transform(Lazy<Integer> key, Lazy<String> value, int partitionId) {
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

  @Override
  public void processPut(Lazy<Integer> key, Lazy<String> value, int partitionId) {
    return;
  }
  
  @Override
  public void close() throws IOException {

  }
}

```

Here's an example `DaVinciRecordTransformerConfig` implementation:
```
DaVinciConfig config = new DaVinciConfig();
DaVinciRecordTransformerConfig recordTransformerConfig =
        new DaVinciRecordTransformerConfig.Builder()
            .setRecordTransformerFunction(StringRecordTransformer::new)
            .build();
config.setRecordTransformerConfig(recordTransformerConfig);
```

### Schema Modification
If you want to modify the Value Schema of the record, here's an example `DaVinciRecordTransformer` implementation:
```
import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.IOException;
import org.apache.avro.Schema;


/**
 * Transforms int values to strings
 */
public class IntToStringRecordTransformer extends DaVinciRecordTransformer<Integer, Integer, String> {
  public IntToStringRecordTransformer(
      int storeVersion,
      Schema keySchema,
      Schema inputValueSchema,
      Schema outputValueSchema,
      DaVinciRecordTransformerConfig recordTransformerConfig) {
    super(storeVersion, keySchema, inputValueSchema, outputValueSchema, recordTransformerConfig);
  }

  @Override
  public DaVinciRecordTransformerResult<String> transform(Lazy<Integer> key, Lazy<Integer> value, int partitionId) {
    String valueStr = value.get().toString();
    String transformedValue = valueStr + "Transformed";
    return new DaVinciRecordTransformerResult<>(DaVinciRecordTransformerResult.Result.TRANSFORMED, transformedValue);
  }

  @Override
  public void processPut(Lazy<Integer> key, Lazy<String> value, int partitionId) {
    return;
  }

  @Override
  public void close() throws IOException {

  }
}
```

Here's an example `DaVinciRecordTransformerConfig` implementation:
```
DaVinciRecordTransformerConfig recordTransformerConfig =
        new DaVinciRecordTransformerConfig.Builder()
            .setRecordTransformerFunction(IntToStringRecordTransformer::new)
            .setOutputValueClass(String.class)
            .setOutputValueSchema(Schema.create(Schema.Type.STRING))
            .build();
config.setRecordTransformerConfig(recordTransformerConfig);
```