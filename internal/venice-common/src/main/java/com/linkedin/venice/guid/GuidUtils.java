package com.linkedin.venice.guid;

import static com.linkedin.venice.ConfigKeys.PUSH_JOB_GUID_LEAST_SIGNIFICANT_BITS;
import static com.linkedin.venice.ConfigKeys.PUSH_JOB_GUID_MOST_SIGNIFICANT_BITS;

import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.ReflectUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.avro.specific.FixedSize;
import org.apache.avro.util.Utf8;


/**
 * Utility class for generating GUIDs.
 *
 * N.B.: This is not meant for high-throughput utilization. If that becomes a use case, we can optimize it further.
 */
public class GuidUtils {
  public static final String GUID_GENERATOR_IMPLEMENTATION = "guid.generator.implementation";
  public static final String DEFAULT_GUID_GENERATOR_IMPLEMENTATION = JavaUtilGuidV4Generator.class.getName();
  public static final String DETERMINISTIC_GUID_GENERATOR_IMPLEMENTATION = DeterministicGuidGenerator.class.getName();
  public static final int GUID_SIZE_IN_BYTES = GUID.class.getAnnotation(FixedSize.class).value(); // 16

  public static GUID getGUID(VeniceProperties properties) {
    return getGuidGenerator(properties).getGuid();
  }

  private static GuidGenerator getGuidGenerator(VeniceProperties properties) {
    String implName = properties.getString(GUID_GENERATOR_IMPLEMENTATION, DEFAULT_GUID_GENERATOR_IMPLEMENTATION);
    GuidGenerator guidGenerator;
    if (implName.equals(JavaUtilGuidV4Generator.class.getName())) {
      guidGenerator = new JavaUtilGuidV4Generator();
    } else if (implName.equals(DeterministicGuidGenerator.class.getName())) {
      guidGenerator = new DeterministicGuidGenerator(
          properties.getLong(PUSH_JOB_GUID_MOST_SIGNIFICANT_BITS),
          properties.getLong(PUSH_JOB_GUID_LEAST_SIGNIFICANT_BITS));
    } else {
      Class<? extends GuidGenerator> implClass = ReflectUtils.loadClass(implName);
      guidGenerator = ReflectUtils.callConstructor(implClass, new Class[0], new Object[0]);
    }
    return guidGenerator;
  }

  static final Charset CHARSET = StandardCharsets.ISO_8859_1;

  public static GUID getGuidFromCharSequence(CharSequence charSequence) {
    GUID guid = new GUID();
    guid.bytes(charSequence.toString().getBytes(CHARSET)); // TODO: Optimize this. It's probably expensive...
    return guid;
  }

  public static String getCharSequenceFromGuid(GUID guid) {
    return new String(guid.bytes(), CHARSET); // TODO: Optimize this. It's probably expensive...
  }

  public static CharSequence guidToUtf8(GUID guid) {
    /** TODO: Consider replacing with {@link GuidUtils#getUtf8FromGuid(GUID)}, which might be more efficient. */
    return new Utf8(getCharSequenceFromGuid(guid));
  }

  public static Utf8 getUtf8FromGuid(GUID guid) {
    /** Adapted from {@link StringCoding#encodeUTF8(byte, byte[], boolean)} */
    byte[] val = guid.bytes();
    if (!hasNegatives(val, 0, val.length)) {
      return new Utf8(val);
    }

    int dp = 0;
    byte[] dst = new byte[val.length << 1];
    for (int sp = 0; sp < val.length; sp++) {
      byte c = val[sp];
      if (c < 0) {
        dst[dp++] = (byte) (0xc0 | ((c & 0xff) >> 6));
        dst[dp++] = (byte) (0x80 | (c & 0x3f));
      } else {
        dst[dp++] = c;
      }
    }

    Utf8 result = new Utf8(dst);
    if (dp != dst.length) {
      result.setByteLength(dp);
    }

    return result;
  }

  private static boolean hasNegatives(byte[] ba, int off, int len) {
    /** Copied from {@link StringCoding#hasNegatives(byte[], int, int)} */
    for (int i = off; i < off + len; i++) {
      if (ba[i] < 0) {
        return true;
      }
    }
    return false;
  }

  public static GUID getGuidFromHex(String hexGuid) {
    GUID guid = new GUID();
    guid.bytes(ByteUtils.fromHexString(hexGuid));
    return guid;
  }

  public static String getHexFromGuid(GUID guid) {
    return ByteUtils.toHexString(guid.bytes());
  }

  public static String getGUIDString() {
    return UUID.randomUUID().toString();
  }
}
