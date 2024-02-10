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
This feature enables applications to transform records as they're being consumed and stored in the Da Vinci Client. 
It's capable of handling records that are compressed and/or chunked.

### Usage
To use the record transformer, you will need to implement the 
[DaVinciRecordTransformer](http://venicedb.org/javadoc/com/linkedin/davinci/client/DaVinciRecordTransformer.html) 
interface and pass the object into 
[setRecordTransformer()](https://venicedb.org/javadoc/com/linkedin/davinci/client/DaVinciConfig.html#setRecordTransformer(com.linkedin.davinci.client.DaVinciRecordTransformer)). 

When a message is being consumed, the 
[DaVinciRecordTransformer](http://venicedb.org/javadoc/com/linkedin/davinci/client/DaVinciRecordTransformer.html) will 
modify the value before it is written to storage.

Here's an example implementation:
```
import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.TransformedRecord;
import com.linkedin.venice.utils.lazy.Lazy;
import org.apache.avro.Schema;


public class StringRecordTransformer
    implements DaVinciRecordTransformer<Integer, String, TransformedRecord<Integer, String>> {
  public Schema getKeyOutputSchema() {
    return Schema.create(Schema.Type.INT);
  }

  public Schema getValueOutputSchema() {
    return Schema.create(Schema.Type.STRING);
  }

  public TransformedRecord<Integer, String> put(Lazy<Integer> key, Lazy<String> value) {
    TransformedRecord<Integer, String> transformedRecord = new TransformedRecord<>();
    transformedRecord.setKey(key.get());
    transformedRecord.setValue(value.get() + "Transformed");
    return transformedRecord;
  }
}
```