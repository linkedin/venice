package com.linkedin.venice.pubsub.api;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.utils.ReflectUtils;
import java.util.HashSet;
import java.util.Set;
import org.testng.annotations.Test;


public class PubSubSecurityProtocolTest {
  private static final Class SPECIFIC_ENUM_CLASS = getFQCN();

  private static Class getFQCN() {
    String simpleClassName = "SecurityProtocol";
    try {
      String apacheKafkaFQCN = "org.apache.kafka.common.security.auth" + "." + simpleClassName;
      Class apacheKafkaSecurityProtocol = ReflectUtils.loadClass(apacheKafkaFQCN);
      if (apacheKafkaSecurityProtocol != null && apacheKafkaSecurityProtocol.isEnum()) {
        return apacheKafkaSecurityProtocol;
      }
    } catch (Exception e) {
      // Expected if not using Apache Kafka.
    }
    try {
      String liKafkaFQCN = "org.apache.kafka.common.protocol" + "." + simpleClassName;
      Class liKafkaSecurityProtocol = ReflectUtils.loadClass(liKafkaFQCN);
      if (liKafkaSecurityProtocol != null && liKafkaSecurityProtocol.isEnum()) {
        return liKafkaSecurityProtocol;
      }
    } catch (Exception e) {
      // Expected if not using LI Kafka...
    }
    throw new IllegalStateException(
        "Neither the Apache Kafka nor LinkedIn Kafka version of " + simpleClassName + " were found on the classpath!");
  }

  /**
   * This is a proof of concept of how we could instantiate the specific enum, though as of now there is no need for it.
   *
   * @return an enum instance specific to the pub sub library present on the class path.
   */
  private static Enum getPubSubSpecificEnum(String enumValueName) {
    return Enum.valueOf(SPECIFIC_ENUM_CLASS, enumValueName);
  }

  /**
   * This test merely checks that our own PubSubSecurityProtocol contains only values which exist in Kafka's own enum.
   * It can be run with the classpath containing either the Apache fork or LinkedIn's fork of Kafka, and succeed.
   */
  @Test
  public void testInstantiation() {
    for (PubSubSecurityProtocol value: PubSubSecurityProtocol.values()) {
      Enum specificEnum = getPubSubSpecificEnum(value.name());
      assertNotNull(specificEnum);
      assertTrue(specificEnum.getClass().isEnum());
      assertEquals(specificEnum.getClass().getSimpleName(), "SecurityProtocol");
      Set<String> expectedPackageNames = new HashSet<>(2);
      expectedPackageNames.add("org.apache.kafka.common.security.auth");
      expectedPackageNames.add("org.apache.kafka.common.protocol");
      assertTrue(expectedPackageNames.contains(specificEnum.getClass().getPackage().getName()));
      assertEquals(specificEnum.name(), value.name());
    }
  }

  @Test
  public void testForName() {
    assertEquals(PubSubSecurityProtocol.forName("plaintext"), PubSubSecurityProtocol.PLAINTEXT);
    assertEquals(PubSubSecurityProtocol.forName("PLAINTEXT"), PubSubSecurityProtocol.PLAINTEXT);
    assertEquals(PubSubSecurityProtocol.forName("Plaintext"), PubSubSecurityProtocol.PLAINTEXT);

    assertEquals(PubSubSecurityProtocol.forName("ssl"), PubSubSecurityProtocol.SSL);
    assertEquals(PubSubSecurityProtocol.forName("SSL"), PubSubSecurityProtocol.SSL);
    assertEquals(PubSubSecurityProtocol.forName("Ssl"), PubSubSecurityProtocol.SSL);

    assertEquals(PubSubSecurityProtocol.forName("sasl_plaintext"), PubSubSecurityProtocol.SASL_PLAINTEXT);
    assertEquals(PubSubSecurityProtocol.forName("SASL_PLAINTEXT"), PubSubSecurityProtocol.SASL_PLAINTEXT);
    assertEquals(PubSubSecurityProtocol.forName("Sasl_Plaintext"), PubSubSecurityProtocol.SASL_PLAINTEXT);

    assertEquals(PubSubSecurityProtocol.forName("sasl_ssl"), PubSubSecurityProtocol.SASL_SSL);
    assertEquals(PubSubSecurityProtocol.forName("SASL_SSL"), PubSubSecurityProtocol.SASL_SSL);
    assertEquals(PubSubSecurityProtocol.forName("Sasl_Ssl"), PubSubSecurityProtocol.SASL_SSL);
  }
}
