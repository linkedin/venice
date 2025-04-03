package com.linkedin.venice.views;

import static com.linkedin.venice.pubsub.api.PubSubMessageHeaders.VENICE_VIEW_PARTITIONS_MAP_HEADER;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.pubsub.api.PubSubMessageHeader;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.ReflectUtils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.writer.ComplexVeniceWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;


public class ViewUtils {
  public static final String PARTITION_COUNT = "sub.partition.count";
  public static final String USE_FAST_KAFKA_OPERATION_TIMEOUT = "use.fast.kafka.operation.timeout";

  public static final String LOG_COMPACTION_ENABLED = "log.compaction.enabled";

  public static final String ETERNAL_TOPIC_RETENTION_ENABLED = "eternal.topic.retention.enabled";

  public static final String NEARLINE_PRODUCER_COMPRESSION_ENABLED = "nearline.producer.compression.enabled";

  public static final String NEARLINE_PRODUCER_COUNT_PER_WRITER = "nearline.producer.count.per.writer";

  public static VeniceView getVeniceView(
      String viewClass,
      Properties params,
      String veniceStoreName,
      Map<String, String> extraParameters) {
    VeniceView view = ReflectUtils.callConstructor(
        ReflectUtils.loadClass(viewClass),
        new Class<?>[] { Properties.class, String.class, Map.class },
        new Object[] { params, veniceStoreName, extraParameters });
    return view;
  }

  public static String flatViewConfigMapString(Map<String, ViewConfig> viewConfigMap) throws JsonProcessingException {
    ObjectMapper mapper = ObjectMapperFactory.getInstance();
    Map<String, String> flatMap = new HashMap<>();
    for (Map.Entry<String, ViewConfig> mapEntry: viewConfigMap.entrySet()) {
      flatMap.put(mapEntry.getKey(), mapper.writeValueAsString(mapEntry.getValue()));
    }
    return mapper.writeValueAsString(flatMap);
  }

  public static Map<String, ViewConfig> parseViewConfigMapString(String flatViewConfigMapString)
      throws JsonProcessingException {
    ObjectMapper mapper = ObjectMapperFactory.getInstance();
    Map<String, String> flatMap = mapper.readValue(flatViewConfigMapString, Map.class);
    Map<String, ViewConfig> viewConfigMap = new HashMap<>();
    for (Map.Entry<String, String> entry: flatMap.entrySet()) {
      viewConfigMap.put(entry.getKey(), mapper.readValue(entry.getValue(), ViewConfig.class));
    }
    return viewConfigMap;
  }

  public static Map<String, VeniceProperties> getViewTopicsAndConfigs(
      Collection<ViewConfig> viewConfigs,
      Properties veniceViewProperties,
      String storeName,
      int version) {
    Map<String, VeniceProperties> viewTopicNamesAndConfigs = new HashMap<>();
    for (ViewConfig rawView: viewConfigs) {
      VeniceView veniceView =
          getVeniceView(rawView.getViewClassName(), veniceViewProperties, storeName, rawView.getViewParameters());
      viewTopicNamesAndConfigs.putAll(veniceView.getTopicNamesAndConfigsForVersion(version));
    }
    return viewTopicNamesAndConfigs;
  }

  public static Map<String, Set<Integer>> extractViewPartitionMap(PubSubMessageHeaders pubSubMessageHeaders) {
    Map<String, Set<Integer>> viewPartitionMap = null;
    PubSubMessageHeader header = pubSubMessageHeaders.get(VENICE_VIEW_PARTITIONS_MAP_HEADER);
    if (header != null) {
      try {
        TypeReference<Map<String, Set<Integer>>> typeReference = new TypeReference<Map<String, Set<Integer>>>() {
        };
        viewPartitionMap = ObjectMapperFactory.getInstance().readValue(header.value(), typeReference);
      } catch (IOException e) {
        throw new VeniceException(
            "Failed to parse view partition map from the record's VENICE_VIEW_PARTITIONS_MAP_HEADER",
            e);
      }
    }
    if (viewPartitionMap == null) {
      throw new VeniceException("Unable to find VENICE_VIEW_PARTITIONS_MAP_HEADER in the record's message headers");
    }
    return viewPartitionMap;
  }

  public static PubSubMessageHeader getViewDestinationPartitionHeader(
      Map<String, Set<Integer>> destinationPartitionMap) {
    if (destinationPartitionMap == null) {
      return null;
    }
    try {
      // We could explore more storage efficient ways to pass this information.
      byte[] value = ObjectMapperFactory.getInstance().writeValueAsBytes(destinationPartitionMap);
      return new PubSubMessageHeader(VENICE_VIEW_PARTITIONS_MAP_HEADER, value);
    } catch (JsonProcessingException e) {
      throw new VeniceException("Failed to serialize view destination partition map", e);
    }
  }

  /**
   * This uses {@link com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper} for field extraction. Therefor it
   * has the same limitations as regular read compute where for Avro-1.9 or above we cannot guarantee the extracted
   * field will be exactly the same as existing field in terms of default value. Since the extracted default value will
   * be in Java format. This shouldn't be an issue for most use cases, but it's something worth to keep in mind about.
   */
  public static String validateAndGenerateProjectionSchema(
      Schema latestValueSchema,
      Set<String> projectionFields,
      String generatedSchemaName) {
    if (latestValueSchema == null) {
      throw new IllegalArgumentException("Latest value schema cannot be null");
    }
    List<Schema.Field> projectionSchemaFields = new LinkedList<>();

    for (String fieldName: projectionFields) {
      Schema.Field field = latestValueSchema.getField(fieldName);
      if (field == null) {
        throw new VeniceException("Field: " + fieldName + " does not exist in latest value schema");
      }
      if (!field.hasDefaultValue()) {
        throw new VeniceException("Default value is required for field: " + fieldName);
      }
      projectionSchemaFields.add(AvroCompatibilityHelper.newField(field).setDoc("").build());
    }

    Schema generatedProjectionSchema =
        Schema.createRecord(generatedSchemaName, "", latestValueSchema.getNamespace(), false);
    generatedProjectionSchema.setFields(projectionSchemaFields);
    return generatedProjectionSchema.toString();
  }

  public static void validateFilterByFields(Schema latestValueSchema, Set<String> filterByFields) {
    if (latestValueSchema == null) {
      throw new IllegalArgumentException("Latest value schema cannot be null");
    }
    for (String fieldName: filterByFields) {
      Schema.Field field = latestValueSchema.getField(fieldName);
      if (field == null) {
        throw new VeniceException("Field: " + fieldName + " does not exist in latest value schema");
      }
    }
  }

  public static void project(GenericRecord inputRecord, GenericRecord resultRecord) {
    Schema.Field inputRecordField;
    for (Schema.Field field: resultRecord.getSchema().getFields()) {
      inputRecordField = inputRecord.getSchema().getField(field.name());
      if (inputRecordField != null) {
        resultRecord.put(field.pos(), inputRecord.get(inputRecordField.pos()));
      }
    }
  }

  /**
   * @param oldValue of the record
   * @param newValue of the record
   * @param filterByFields to perform change filter on
   * @return boolean to decide on the filter result. i.e. true is to keep and false is to be filtered .
   */
  public static boolean changeFilter(
      GenericRecord oldValue,
      GenericRecord newValue,
      List<String> filterByFields,
      String viewName) {
    if (oldValue == null) {
      return true;
    }
    if (newValue == null) {
      throw new VeniceException("Cannot perform filter because new value is null for view: " + viewName);
    }
    boolean changed = false;
    for (String fieldName: filterByFields) {
      Schema fieldSchema = oldValue.getSchema().getField(fieldName).schema();
      if (GenericData.get().compare(oldValue.get(fieldName), newValue.get(fieldName), fieldSchema) != 0) {
        changed = true;
        break;
      }
    }
    return changed;
  }

  public static void configureWriterForProjection(
      ComplexVeniceWriter<byte[], byte[], byte[]> complexVeniceWriter,
      String viewName,
      Lazy<VeniceCompressor> compressor,
      String projectionSchemaString) {
    Schema projectionSchema = new Schema.Parser().parse(projectionSchemaString);
    Lazy<RecordSerializer<GenericRecord>> serializer =
        Lazy.of(() -> FastSerializerDeserializerFactory.getFastAvroGenericSerializer(projectionSchema));
    complexVeniceWriter.configureWriterForProjection(projectionSchemaString, (projectionRecord) -> {
      try {
        return compressor.get().compress(serializer.get().serialize(projectionRecord));
      } catch (IOException e) {
        throw new VeniceException("Projection failed due to compression error for view: " + viewName, e);
      }
    });
  }
}
