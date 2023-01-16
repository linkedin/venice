package com.linkedin.davinci.config;

import com.linkedin.venice.exceptions.VeniceException;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class StoreBackendConfig {
  private static final Logger LOGGER = LogManager.getLogger(StoreBackendConfig.class);

  public static final String CONFIG_DIRECTORY = "config";
  public static final String IS_MANAGED = "managed";

  private final File file;
  private final Properties props = new Properties();

  public StoreBackendConfig(String baseDataPath, String storeName) {
    this.file = Paths.get(baseDataPath, CONFIG_DIRECTORY, storeName).toFile();
    this.file.getParentFile().mkdirs();
    if (this.file.exists()) {
      load();
    } else {
      setManaged(true);
    }
  }

  public static List<String> listConfigs(String baseDataPath) {
    File configDirectory = Paths.get(baseDataPath, CONFIG_DIRECTORY).toFile();
    if (configDirectory.exists() && configDirectory.isDirectory()) {
      String[] files = configDirectory.list();
      if (files == null) {
        throw new VeniceException("Failed to list config files in " + configDirectory.getAbsolutePath());
      }
      return Arrays.asList(files);
    }
    return Collections.emptyList();
  }

  public final void load() {
    try (InputStream in = new FileInputStream(file)) {
      props.load(in);
      LOGGER.info("Loaded store config from {}: {}", file.getAbsolutePath(), props);
    } catch (Exception e) {
      throw new VeniceException("Unable to read store backend config", e);
    }
  }

  public void store() {
    try (OutputStream out = new BufferedOutputStream(new FileOutputStream(file))) {
      props.store(out, null);
      LOGGER.info("Stored store config at {}: {}", file.getAbsolutePath(), props);
    } catch (Exception e) {
      throw new VeniceException("Unable to write store backend config", e);
    }
  }

  public void delete() {
    file.delete();
    LOGGER.info("Deleted store config from {}", file.getAbsolutePath());
  }

  public String getStoreName() {
    return file.getName();
  }

  public boolean isManaged() {
    return Boolean.parseBoolean(props.getProperty(IS_MANAGED));
  }

  public void setManaged(boolean isManaged) {
    props.setProperty(IS_MANAGED, String.valueOf(isManaged));
  }
}
