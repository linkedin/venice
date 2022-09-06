package com.linkedin.venice.hadoop.ssl;

import com.linkedin.venice.utils.ReflectUtils;
import java.util.Properties;
import org.apache.hadoop.security.Credentials;


public interface SSLConfigurator {
  Properties setupSSLConfig(Properties properties, Credentials userCredentials);

  static SSLConfigurator getSSLConfigurator(String className) {
    Class<SSLConfigurator> configuratorClass = ReflectUtils.loadClass(className);
    return ReflectUtils.callConstructor(configuratorClass, null, null);
  }
}
