---
layout: default
title: Da Vinci Record Transformer
parent: Da Vinci Client
grand_parent: User Guides
permalink: /docs/user_guide/da_vinci_client/da_vinci_record_transformer
---

# Da Vinci Record Transformer

This feature enables applications to transform records as they're being consumed and stored in the Da Vinci Client. It's capable of handling records that are compressed and/or chunked.

## Usage
To use the record transformer, you will need to implement the `DaVinciRecordTransformer` interface and pass it into the `StoreIngestionTask` constructor. When a message is being consumed, the `DaVinciRecordTransformer` will modify the value before it is written to storage.

Here's an example implementation:
```
import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.TransformedRecord;
import com.linkedin.venice.utils.lazy.Lazy;
import org.apache.avro.Schema;


public class StringRecordTransformer
    implements DaVinciRecordTransformer<Integer, Object, TransformedRecord<Integer, String>> {
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