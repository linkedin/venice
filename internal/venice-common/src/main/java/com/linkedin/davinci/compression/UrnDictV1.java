package com.linkedin.davinci.compression;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.SparseConcurrentList;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class implements a URN dictionary for encoding and decoding URNs.
 * The dictionary maps URN types to integer IDs to reduce the size of URNs when stored
 * in a data store or transmitted over the network.
 */
public class UrnDictV1 {
  private static final Logger LOGGER = LogManager.getLogger(UrnDictV1.class);

  public static class EncodedUrn {
    public final int urnTypeId;
    public final String urnRemainder;

    public EncodedUrn(int urnTypeId, String urnRemainder) {
      this.urnTypeId = urnTypeId;
      this.urnRemainder = urnRemainder;
    }

    public static EncodedUrn unknownUrn(String urn) {
      return new EncodedUrn(UNKNOWN_URN_TYPE_ID, urn);
    }
  }

  public static final int DEFAULT_MAX_DICT_SIZE = 1024;
  public static final String URN_PREFIX = "urn:li:";
  public static final Character URN_SEPARATOR = ':';
  public static final int URN_PREFIX_LENGTH = URN_PREFIX.length();
  public static final int UNKNOWN_URN_TYPE_ID = -1;
  public static final Schema DICT_SCHEMA = Schema.createMap(Schema.create(Schema.Type.INT));
  public static final RecordSerializer<Object> DICT_SERIALIZER =
      FastSerializerDeserializerFactory.getFastAvroGenericSerializer(DICT_SCHEMA);
  public static final RecordDeserializer<Object> DICT_DESERIALIZER =
      FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(DICT_SCHEMA, DICT_SCHEMA);

  private final Map<String, Integer> urnToIdMap; // <urn, id>
  private final List<String> idToUrnList; // <id, urn>
  private final AtomicInteger nextUrnTypeId;
  private final int maxDictSize;

  private UrnDictV1(Map<String, Integer> urnToIdMap, int maxDictSize) {
    validateDict(urnToIdMap);
    if (urnToIdMap instanceof VeniceConcurrentHashMap) {
      this.urnToIdMap = urnToIdMap;
    } else {
      this.urnToIdMap = new VeniceConcurrentHashMap<>(urnToIdMap.size());
      this.urnToIdMap.putAll(urnToIdMap);
    }
    this.maxDictSize = maxDictSize;
    this.nextUrnTypeId = new AtomicInteger(urnToIdMap.size());
    this.idToUrnList = new SparseConcurrentList<>();
    urnToIdMap.forEach((urn, id) -> idToUrnList.set(id, urn));
  }

  /**
   * This function will validate the dictionary in the following ways:
   * 1. Make sure the dictionary is not null.
   * 2. Make sure the mapped ids are in the range of [0, dict.size() - 1).
   * 3. Make sure there are no duplicate ids.
   * When validation fail, it will throw an VeniceException.
   */
  public static void validateDict(Map<String, Integer> dict) {
    if (dict == null) {
      throw new VeniceException("The URN dictionary is null");
    }
    int dictSize = dict.size();
    boolean[] idSeen = new boolean[dictSize];
    for (Map.Entry<String, Integer> entry: dict.entrySet()) {
      String urn = entry.getKey();
      Integer id = entry.getValue();
      if (id == null) {
        throw new VeniceException("The URN dictionary contains a null id for urn: " + urn);
      }
      if (id < 0 || id >= dictSize) {
        throw new VeniceException(
            "The URN dictionary contains an invalid id: " + id + " for urn: " + urn + ", valid range is [0, "
                + (dictSize - 1) + "]");
      }
      if (idSeen[id]) {
        throw new VeniceException("The URN dictionary contains a duplicate id: " + id + " for urn: " + urn);
      }
      idSeen[id] = true;
    }
  }

  public Map<String, Integer> getUrnToIdMap() {
    return urnToIdMap;
  }

  public static UrnDictV1 loadDict(Map<String, Integer> dict) {
    return loadDict(dict, DEFAULT_MAX_DICT_SIZE);
  }

  public static UrnDictV1 loadDict(Map<String, Integer> dict, int maxDictSize) {
    if (dict == null) {
      throw new VeniceException("Cannot load a null dictionary");
    }
    if (maxDictSize <= 0) {
      throw new VeniceException("Invalid dictionary size: " + maxDictSize);
    }
    return new UrnDictV1(dict, maxDictSize);
  }

  public static UrnDictV1 loadDict(ByteBuffer dict) {
    return loadDict(dict, DEFAULT_MAX_DICT_SIZE);
  }

  public static UrnDictV1 loadDict(ByteBuffer dict, int maxDictSize) {
    return loadDict(ByteUtils.extractByteArray(dict), maxDictSize);
  }

  public static UrnDictV1 loadDict(byte[] dict, int maxDictSize) {
    if (dict == null || dict.length == 0) {
      throw new VeniceException("Cannot load an empty dictionary");
    }
    if (maxDictSize <= 0) {
      throw new VeniceException("Invalid dictionary size: " + maxDictSize);
    }
    Map<CharSequence, Integer> deserializedDict = (Map<CharSequence, Integer>) DICT_DESERIALIZER.deserialize(dict);
    // Convert CharSequence keys to String keys
    Map<String, Integer> finalDict = new VeniceConcurrentHashMap<>();
    for (Map.Entry<CharSequence, Integer> entry: deserializedDict.entrySet()) {
      finalDict.put(entry.getKey().toString(), entry.getValue());
    }
    return new UrnDictV1(finalDict, maxDictSize);
  }

  public byte[] serializeDict() {
    return DICT_SERIALIZER.serialize(urnToIdMap);
  }

  public static UrnDictV1 trainDict(List<String> urns, int dictSize) {
    if (dictSize <= 0) {
      throw new VeniceException("Invalid dictionary size: " + dictSize);
    }
    Map<String, Integer> dict = new VeniceConcurrentHashMap<>();
    int urnIndex = 0;
    for (String urn: urns) {
      if (!urn.startsWith(URN_PREFIX)) {
        // No match
        continue;
      }
      int coloIndex = urn.indexOf(URN_SEPARATOR, URN_PREFIX_LENGTH);
      if (coloIndex <= 0) {
        // Invalid urn
        continue;
      }
      // Extract the urn type
      String urnType = urn.substring(URN_PREFIX_LENGTH, coloIndex);

      if (!dict.containsKey(urnType)) {
        dict.put(urnType, urnIndex);
        urnIndex++;
        if (dict.size() >= dictSize) {
          break;
        }
      }
    }
    return new UrnDictV1(dict, dictSize);
  }

  public EncodedUrn encodeUrn(String urn) {
    return encodeUrn(urn, true);
  }

  public EncodedUrn encodeUrn(String urn, boolean updateDictByUnknownUrn) {
    if (!urn.startsWith(URN_PREFIX)) {
      return EncodedUrn.unknownUrn(urn);
    }
    int coloIndex = urn.indexOf(URN_SEPARATOR, URN_PREFIX_LENGTH);
    if (coloIndex <= 0) {
      return EncodedUrn.unknownUrn(urn);
    }
    String urnType = urn.substring(URN_PREFIX_LENGTH, coloIndex);

    /**
     *  If the urn type is already present in the dictionary, return the corresponding id.
     *  If the urn type is not present, we will add it to the dictionary if
     *  the nextUrnTypeId is less than the maxDictSize.
     *  If the nextUrnTypeId is greater than or equal to maxDictSize, we will return an unknown URN.
     *
     *  With this approach, we can build the dictionary dynamically as we encounter new URNs.
      */
    Integer urnTypeId;
    if (updateDictByUnknownUrn) {
      urnTypeId = urnToIdMap.computeIfAbsent(urnType, k -> {
        if (nextUrnTypeId.get() >= maxDictSize) {
          return null;
        }
        int nextId = nextUrnTypeId.getAndIncrement();
        if (nextId >= maxDictSize) {
          return null;
        }
        idToUrnList.set(nextId, urnType);
        return nextId;
      });
    } else {
      urnTypeId = urnToIdMap.get(urnType);
    }

    if (urnTypeId == null) {
      return EncodedUrn.unknownUrn(urn);
    }
    String urnRemainder = urn.substring(coloIndex + 1);
    return new EncodedUrn(urnTypeId, urnRemainder);
  }

  public String decodeUrn(EncodedUrn urn) {
    if (urn.urnTypeId == UNKNOWN_URN_TYPE_ID) {
      return urn.urnRemainder;
    }
    if (urn.urnTypeId < 0 || urn.urnTypeId >= urnToIdMap.size()) {
      throw new VeniceException("Invalid urn type id: " + urn.urnTypeId);
    }
    String urnType = idToUrnList.get(urn.urnTypeId);
    if (urnType == null) {
      throw new VeniceException("Invalid urn type id: " + urn.urnTypeId);
    }
    return URN_PREFIX + urnType + URN_SEPARATOR + urn.urnRemainder;
  }
}
