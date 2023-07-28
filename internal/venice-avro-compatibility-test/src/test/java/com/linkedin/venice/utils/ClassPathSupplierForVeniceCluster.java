package com.linkedin.venice.utils;

import com.linkedin.venice.exceptions.VeniceException;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ScanResult;
import java.io.File;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import nonapi.io.github.classgraph.utils.JarUtils;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This utility is used to remove the existing Avro jars and prepend a specific Avro jar version to the classpath,
 * to control which version the Venice Cluster will use. The version used is controlled by {@link #AVRO_JAR_FILE}.
 */
public class ClassPathSupplierForVeniceCluster implements Supplier<String> {
  private static final Logger LOGGER = LogManager.getLogger(ClassPathSupplierForVeniceCluster.class);

  private static final String AVRO_JAR_FILE = "avro-1.10.2.jar";

  @Override
  public String get() {
    try (ScanResult scanResult = new ClassGraph().scan()) {
      Set<File> classpathDirs = new LinkedHashSet<>();
      for (File file: scanResult.getClasspathFiles()) {
        LOGGER.info(file);
        if (file.isDirectory() || file.getName().equals("*") || file.getAbsolutePath().contains(".gradle")) {
          classpathDirs.add(file);
        } else {
          classpathDirs.add(new File(file.getParent(), "*"));
        }
      }

      List<File> paths = new LinkedList<>();
      File existingAvroJarFile = null;
      try {
        /**
         * Get rid of the current Avro libraries.
         */
        for (File file: classpathDirs) {
          if (!file.getName().contains("avro-1.")) {
            paths.add(file);
          } else {
            existingAvroJarFile = file;
          }
        }
        if (existingAvroJarFile == null) {
          throw new VeniceException("There should be some existing Avro lib in the class path");
        }
        paths.add(extractAvroJarFileBasedOnExistingAvroJarFile(existingAvroJarFile));
      } catch (Exception e) {
        throw new VeniceException("Failed to compose class path", e);
      }
      return JarUtils.pathElementsToPathStr(paths);
    }
  }

  /**
   * Append {@link #AVRO_JAR_FILE} to the classpath, which is the one being used by the backend.
   */
  private File extractAvroJarFileBasedOnExistingAvroJarFile(File existingAvroJarFile) {
    LOGGER.info("Existing avro jar file: {}", existingAvroJarFile.getAbsolutePath());
    if (existingAvroJarFile.getName().equals(AVRO_JAR_FILE)) {
      return existingAvroJarFile;
    }
    /**
     * The file path should be in the following way:
     * .../org.apache.avro/avro/1.4.1/3548c0bc136e71006f3fc34e22d34a29e5069e50/avro-1.4.1.jar
     * And the target file should be here:
     * /org.apache.avro/avro/1.10.2/.../avro-1.10.2.jar
     */
    File avroRootDir = existingAvroJarFile.getParentFile().getParentFile().getParentFile();
    Collection<File> jarFiles = FileUtils.listFiles(avroRootDir, new String[] { "jar" }, true);
    for (File jarFile: jarFiles) {
      if (jarFile.getName().equals(AVRO_JAR_FILE)) {
        LOGGER.info("Found the jar file: {} for {}", jarFile.getAbsolutePath(), AVRO_JAR_FILE);
        return jarFile;
      }
    }
    throw new VeniceException("Failed to find out " + AVRO_JAR_FILE + " in the existing class path");
  }
}
