package com.linkedin.venice.guid;

import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.utils.ReflectUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.UUID;
import org.apache.avro.specific.FixedSize;
import java.nio.charset.Charset;

/**
 * Utility class for generating GUIDs.
 *
 * N.B.: This is not meant for high-throughput utilization. If that becomes a use case, we can optimize it further.
 */
public class GuidUtils {
  public static final String GUID_GENERATOR_IMPLEMENTATION = "guid.generator.implementation";
  public static final String DEFAULT_GUID_GENERATOR_IMPLEMENTATION = JavaUtilGuidV4Generator.class.getName();
  public static final int GUID_SIZE_IN_BYTES = GUID.class.getAnnotation(FixedSize.class).value(); // 16

  public static GUID getGUID(VeniceProperties properties) {
    return getGuidGenerator(properties).getGuid();
  }

  private static GuidGenerator getGuidGenerator(VeniceProperties properties) {
    String implName = properties.getString(GUID_GENERATOR_IMPLEMENTATION, DEFAULT_GUID_GENERATOR_IMPLEMENTATION);
    GuidGenerator guidGenerator = null;
    if (implName.equals(JavaUtilGuidV4Generator.class.getName())) {
      guidGenerator = new JavaUtilGuidV4Generator();
    } else {
      Class implClass = ReflectUtils.loadClass(implName);
      guidGenerator = ReflectUtils.<GuidGenerator>callConstructor(implClass, new Class[0], new Object[0]);
    }
    return guidGenerator;
  }

  static final Charset CHARSET = Charset.forName("ISO-8859-1");

  public static GUID getGuidFromCharSequence(CharSequence charSequence) {
    GUID guid = new GUID();
    guid.bytes(charSequence.toString().getBytes(CHARSET)); // TODO: Optimize this. It's probably expensive...
    return guid;
  }

  public static CharSequence getCharSequenceFromGuid(GUID guid) {
    return new String(guid.bytes(), CHARSET); // TODO: Optimize this. It's probably expensive...
  }

  public static String getGUIDString(){
    return UUID.randomUUID().toString();
  }
}
