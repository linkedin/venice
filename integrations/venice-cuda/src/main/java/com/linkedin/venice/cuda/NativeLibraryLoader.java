package com.linkedin.venice.cuda;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;


/**
 * Utility class to load native libraries from JAR resources
 */
public class NativeLibraryLoader {
  private static boolean loaded = false;

  public static synchronized void loadVeniceGPUTransformer() {
    if (loaded) {
      return;
    }

    String os = System.getProperty("os.name").toLowerCase();
    String arch = System.getProperty("os.arch").toLowerCase();

    String osName;
    String libName;

    if (os.contains("linux")) {
      osName = "linux";
      libName = "libvenice_gpu_transformer.so";
    } else if (os.contains("mac")) {
      osName = "darwin";
      libName = "libvenice_gpu_transformer.dylib";
    } else {
      throw new UnsupportedOperationException("Unsupported OS: " + os);
    }

    // Determine architecture
    String archName;
    if (arch.contains("amd64") || arch.contains("x86_64")) {
      archName = "x86_64";
    } else {
      throw new UnsupportedOperationException("Unsupported architecture: " + arch);
    }

    String resourcePath = "/native/" + osName + "-" + archName + "/" + libName;

    try {
      // Try to load from resources first
      InputStream is = NativeLibraryLoader.class.getResourceAsStream(resourcePath);
      if (is != null) {
        File tempFile = File.createTempFile("venice_gpu_transformer", libName.substring(libName.lastIndexOf('.')));
        tempFile.deleteOnExit();

        Files.copy(is, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        is.close();

        System.load(tempFile.getAbsolutePath());
        loaded = true;
        return;
      }
    } catch (IOException e) {
      // Fall back to system library loading
    }

    // Fall back to loading from system library path
    System.loadLibrary("venice_gpu_transformer");
    loaded = true;
  }
}
