package com.linkedin.venice.utils;

import static com.linkedin.venice.HttpConstants.LOCALHOST;

import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.exceptions.ConfigurationException;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.helix.Replica;
import com.linkedin.venice.helix.ResourceAssignment;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Helper functions
 */
public class Utils {
  private static final Logger LOGGER = LogManager.getLogger(Utils.class);

  public static final String WILDCARD_MATCH_ANY = "*";
  public static final String NEW_LINE_CHAR = System.lineSeparator();
  public static final AtomicBoolean SUPPRESS_SYSTEM_EXIT = new AtomicBoolean();

  /**
   * Print an error and exit with error code 1
   *
   * @param message The error to print
   */
  public static void exit(String message) {
    exit(message, 1);
  }

  /**
   * Print an error and exit with the given error code
   *
   * @param message The error to print
   * @param exitCode The error code to exit with
   */
  public static void exit(String message, int exitCode) {
    System.err.println(message);
    if (SUPPRESS_SYSTEM_EXIT.get()) {
      throw new RuntimeException(message + ", exitCode=" + exitCode);
    }
    System.exit(exitCode);
  }

  /**
   * Run "function" on "t" if "t" is not null
   */
  public static <T> void computeIfNotNull(T t, Consumer<T> function) {
    if (t != null) {
      function.accept(t);
    }
  }

  /**
   *  Given a filePath, reads into a Venice Props object
   *  @param configFileName - String path to a properties file
   *  @return A @Props object with the given configurations
   * */
  public static VeniceProperties parseProperties(String configFileName) throws IOException {
    Properties props = new Properties();
    try (FileInputStream inputStream = new FileInputStream(configFileName)) {
      props.load(inputStream);
    }
    return new VeniceProperties(props);
  }

  /**
   * Generate VeniceProperties object from a given directory, file.
   *
   * @param directory directory that contains the Property file
   * @param fileName fileName of the Property file
   * @param isFileOptional set this to true if the file is optional. If
   *                       file is missing and set to true, empty property
   *                       will be returned. If file is missing and set
   *                       to false, this will throw an exception.
   * @return
   * @throws Exception
   */
  public static VeniceProperties parseProperties(String directory, String fileName, boolean isFileOptional)
      throws IOException {
    String propsFilePath = directory + File.separator + fileName;

    File propsFile = new File(propsFilePath);
    boolean fileExists = propsFile.exists();
    if (!fileExists) {
      if (isFileOptional) {
        return VeniceProperties.empty();
      } else {
        String fullFilePath = Utils.getCanonicalPath(propsFilePath);
        throw new ConfigurationException(fullFilePath + " does not exist.");
      }
    }

    if (!Utils.isReadableFile(propsFilePath)) {
      String fullFilePath = Utils.getCanonicalPath(propsFilePath);
      throw new ConfigurationException(fullFilePath + " is not a readable configuration file.");
    }

    return Utils.parseProperties(propsFilePath);
  }

  /**
   * Given a .property file, reads into a Venice Props object
   * @param propertyFile The .property file
   * @return A @Props object with the given properties
   * @throws Exception  if File not found or not accessible
   */
  public static VeniceProperties parseProperties(File propertyFile) throws IOException {
    Properties props = new Properties();
    try (FileInputStream inputStream = new FileInputStream(propertyFile)) {
      props.load(inputStream);
    }
    return new VeniceProperties(props);
  }

  /**
   * Check if a directory exists and is readable
   *
   * @param d  The directory
   * @return true iff the argument is a readable directory
   */
  public static boolean isReadableDir(File d) {
    return d.exists() && d.isDirectory() && d.canRead();
  }

  /**
   * Check if a directory exists and is readable
   *
   * @param dirName The directory name
   * @return true iff the argument is the name of a readable directory
   */
  public static boolean isReadableDir(String dirName) {
    return isReadableDir(new File(dirName));
  }

  /**
   * Check if a file exists and is readable
   *
   * @param fileName
   * @return true iff the argument is the name of a readable file
   */
  public static boolean isReadableFile(String fileName) {
    return isReadableFile(new File(fileName));
  }

  /**
   * Check if a file exists and is readable
   * @param f The file
   * @return true iff the argument is a readable file
   */
  public static boolean isReadableFile(File f) {
    return f.exists() && f.isFile() && f.canRead();
  }

  /**
   * Get the full Path of the file. Useful in logging/error output
   *
   * @param fileName
   * @return canonicalPath of the file.
   */
  public static String getCanonicalPath(String fileName) {
    try {
      return new File(fileName).getCanonicalPath();
    } catch (IOException ex) {
      return fileName;
    }
  }

  public static boolean directoryExists(String dataDirectory) {
    return Files.isDirectory(Paths.get(dataDirectory));
  }

  private static boolean localhost = false;

  /**
   * The ssl certificate we have for unit tests has the hostname "localhost".  Any tests that rely on this certificate
   * require that the hostname of the machine match the hostname of the certificate.  This method lets us globally assert
   * that the hostname for the machine should resolve to "localhost".  We can call this method at the start of any
   * tests that require hostnames to resolve to "localhost"
   *
   * It's not ideal to put this as state in a Utils class, we can revisit if we come up with a better way to do it
   */
  public static void thisIsLocalhost() {
    localhost = true;
  }

  /**
   * Get the node's host name.
   * @return current node's host name.
   */
  public static String getHostName() {
    if (localhost) {
      return LOCALHOST;
    }
    try {
      String hostName = InetAddress.getLocalHost().getHostName();
      LOGGER.info("Resolved local hostname from InetAddress.getLocalHost() {}", hostName);
      if (StringUtils.isEmpty(hostName)) {
        throw new VeniceException("Unable to get the hostname.");
      }
      return hostName;
    } catch (UnknownHostException e) {
      e.printStackTrace();
      throw new VeniceException("Unable to get the hostname.", e);
    }
  }

  /***
   * Sleep until number of milliseconds have passed, or the operation is interrupted.  This method will swallow the
   * InterruptedException and terminate, if this is used in a loop it may become difficult to cleanly break out
   * of the loop.
   *
   * @param millis
   * @return true on success and false if sleep was interrupted
   */
  public static boolean sleep(long millis) {
    return LatencyUtils.sleep(millis);
  }

  public static int parseIntFromString(String value, String fieldName) {
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new VeniceHttpException(
          HttpStatus.SC_BAD_REQUEST,
          fieldName + " must be an integer, but value: " + value,
          e,
          ErrorType.BAD_REQUEST);
    }
  }

  public static long parseLongFromString(String value, String fieldName) {
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      throw new VeniceHttpException(
          HttpStatus.SC_BAD_REQUEST,
          fieldName + " must be a long, but value: " + value,
          e,
          ErrorType.BAD_REQUEST);
    }
  }

  /**
   * Since {@link Boolean#parseBoolean(String)} does not throw exception and will always return 'false' for
   * any string that are not equal to 'true', We validate the string by our own.
   */
  public static boolean parseBooleanFromString(String value, String fieldName) {
    if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
      return Boolean.valueOf(value);
    } else {
      throw new VeniceHttpException(
          HttpStatus.SC_BAD_REQUEST,
          fieldName + " must be a boolean, but value: " + value,
          ErrorType.BAD_REQUEST);
    }
  }

  /**
   * For String-String key-value map config, we expect the command-line interface users to use "key1=value1,key2=value2,..."
   * format to represent it. This method deserialize it to String-String map.
   */
  public static Map<String, String> parseCommaSeparatedStringMapFromString(String value, String fieldName) {
    try {
      Map<String, String> map = new HashMap<>();
      Arrays.stream(value.split(",")).map(s -> s.split("=")).forEach(strings -> map.put(strings[0], strings[1]));
      return map;
    } catch (Exception e) {
      throw new VeniceException(fieldName + " must be key value pairs separated by comma, but value: " + value);
    }
  }

  public static String getHelixNodeIdentifier(String hostname, int port) {
    return hostname + "_" + port;
  }

  public static String parseHostFromHelixNodeIdentifier(String nodeId) {
    return nodeId.substring(0, nodeId.lastIndexOf('_'));
  }

  public static int parsePortFromHelixNodeIdentifier(String nodeId) {
    return parseIntFromString(nodeId.substring(nodeId.lastIndexOf('_') + 1), "port");
  }

  /**
   * Utility function to get schemas out of embedded resources.
   *
   * @param resourcePath The path of the file under the src/main/resources directory
   * @return the {@link org.apache.avro.Schema} instance corresponding to the file at {@param resourcePath}
   * @throws IOException
   */
  public static Schema getSchemaFromResource(String resourcePath) throws IOException {
    ClassLoader classLoader = Utils.class.getClassLoader();
    try (InputStream inputStream = classLoader.getResourceAsStream(resourcePath)) {
      if (inputStream == null) {
        throw new IOException("Resource path '" + resourcePath + "' does not exist!");
      }
      String schemaString = IOUtils.toString(inputStream);
      Schema schema = Schema.parse(schemaString);
      LOGGER.info("Loaded schema from resource path: {}", resourcePath);
      LOGGER.debug("Schema literal:\n{}", schema.toString(true));
      return schema;
    }
  }

  public static Map<Integer, Schema> getAllSchemasFromResources(AvroProtocolDefinition protocolDef) {
    final int SENTINEL_PROTOCOL_VERSION_USED_FOR_UNDETECTABLE_COMPILED_SCHEMA =
        InternalAvroSpecificSerializer.SENTINEL_PROTOCOL_VERSION_USED_FOR_UNDETECTABLE_COMPILED_SCHEMA;
    final int SENTINEL_PROTOCOL_VERSION_USED_FOR_UNVERSIONED_PROTOCOL =
        InternalAvroSpecificSerializer.SENTINEL_PROTOCOL_VERSION_USED_FOR_UNVERSIONED_PROTOCOL;
    int currentProtocolVersion;
    if (protocolDef.currentProtocolVersion.isPresent()) {
      int currentProtocolVersionAsInt = protocolDef.currentProtocolVersion.get();
      if (currentProtocolVersionAsInt == SENTINEL_PROTOCOL_VERSION_USED_FOR_UNDETECTABLE_COMPILED_SCHEMA
          || currentProtocolVersionAsInt == SENTINEL_PROTOCOL_VERSION_USED_FOR_UNVERSIONED_PROTOCOL
          || currentProtocolVersionAsInt > Byte.MAX_VALUE) {
        throw new IllegalArgumentException(
            "Improperly defined protocol! Invalid currentProtocolVersion: " + currentProtocolVersionAsInt);
      }
      currentProtocolVersion = currentProtocolVersionAsInt;
    } else {
      currentProtocolVersion = SENTINEL_PROTOCOL_VERSION_USED_FOR_UNVERSIONED_PROTOCOL;
    }

    byte compiledProtocolVersion = SENTINEL_PROTOCOL_VERSION_USED_FOR_UNDETECTABLE_COMPILED_SCHEMA;
    String className = protocolDef.getClassName();
    Map<Integer, Schema> protocolSchemaMap = new TreeMap<>();
    int initialVersion;
    if (currentProtocolVersion > 0) {
      initialVersion = 1; // TODO: Consider making configurable if we ever need to fully deprecate some old versions
    } else {
      initialVersion = currentProtocolVersion;
    }
    final String sep = "/"; // TODO: Make sure that jar resources are always forward-slash delimited, even on Windows
    int version = initialVersion;
    while (true) {
      String versionPath = "avro" + sep;
      if (currentProtocolVersion != SENTINEL_PROTOCOL_VERSION_USED_FOR_UNVERSIONED_PROTOCOL) {
        versionPath += className + sep + "v" + version + sep;
      }
      versionPath += className + ".avsc";
      try {
        Schema schema = Utils.getSchemaFromResource(versionPath);
        protocolSchemaMap.put(version, schema);
        if (schema.equals(protocolDef.getCurrentProtocolVersionSchema())) {
          compiledProtocolVersion = (byte) version;
          break;
        }
        if (currentProtocolVersion == SENTINEL_PROTOCOL_VERSION_USED_FOR_UNVERSIONED_PROTOCOL) {
          break;
        } else if (currentProtocolVersion > 0) {
          // Positive version protocols should continue looking "up" for the next version
          version++;
        } else {
          // And vice-versa for negative version protocols
          version--;
        }
      } catch (IOException e) {
        // Then the schema was not found at the requested path
        if (version == initialVersion) {
          throw new VeniceException("Failed to initialize schemas! No resource found at: " + versionPath, e);
        } else {
          break;
        }
      }
    }

    /** Ensure that we are using Avro properly. */
    if (compiledProtocolVersion == SENTINEL_PROTOCOL_VERSION_USED_FOR_UNDETECTABLE_COMPILED_SCHEMA) {
      throw new VeniceException(
          "Failed to identify which version is currently compiled for " + protocolDef.name()
              + ". This could happen if the avro schemas have been altered without recompiling the auto-generated classes"
              + ", or if the auto-generated classes were edited directly instead of generating them from the schemas.");
    }

    /**
     * Verify that the intended current protocol version defined in the {@link AvroProtocolDefinition} is available
     * in the jar's resources and that it matches the auto-generated class that is actually compiled.
     *
     * N.B.: An alternative design would have been to assume that what is compiled is the intended version, but we
     * are instead making this a very explicit choice by requiring the change in both places and failing loudly
     * when there is an inconsistency.
     */
    Schema intendedCurrentProtocol = protocolSchemaMap.get((int) currentProtocolVersion);
    if (intendedCurrentProtocol == null) {
      throw new VeniceException(
          "Failed to get schema for current version: " + currentProtocolVersion + " class: " + className);
    } else if (!intendedCurrentProtocol.equals(protocolDef.getCurrentProtocolVersionSchema())) {
      throw new VeniceException(
          "The intended protocol version (" + currentProtocolVersion
              + ") does not match the compiled protocol version (" + compiledProtocolVersion + ").");
    }

    return protocolSchemaMap;
  }

  /**
   * Verify that is the new status allowed to be used.
   */
  public static boolean verifyTransition(ExecutionStatus newStatus, ExecutionStatus... allowed) {
    return Arrays.asList(allowed).contains(newStatus);
  }

  public static List<String> parseCommaSeparatedStringToList(String rawString) {
    String[] strArray = rawString.split(",\\s*");
    if (strArray.length < 1) {
      throw new VeniceException("Invalid input: " + rawString);
    }
    return Arrays.asList(strArray);
  }

  public static Set<String> parseCommaSeparatedStringToSet(String rawString) {
    if (rawString == null || rawString.length() == 0) {
      return Collections.emptySet();
    }
    return Utils.setOf(rawString.split(",\\s*"));
  }

  /**
   * Computes the percentage, taking care of division by 0
   */
  public static double getRatio(long rawNum, long total) {
    return total == 0 ? 0.0d : rawNum / (double) total;
  }

  /**
   * @param value the double value to be rounded
   * @param precision the number of decimal places by which to round
   * @return {@param value} rounded by {@param precision} decimal places
   */
  public static double round(double value, int precision) {
    int scale = (int) Math.pow(10, precision);
    return (double) Math.round(value * scale) / scale;
  }

  private static final String[] LARGE_NUMBER_SUFFIXES = { "", "K", "M", "B", "T" };

  public static String makeLargeNumberPretty(long largeNumber) {
    if (largeNumber < 2) {
      return "" + largeNumber;
    }
    double doubleNumber = (double) largeNumber;
    double numberOfDigits = Math.ceil(Math.log10(doubleNumber));
    int suffixIndex = Math.min((int) Math.floor((numberOfDigits - 1) / 3), LARGE_NUMBER_SUFFIXES.length - 1);
    double divider = Math.pow(1000, suffixIndex);
    int prettyNumber = (int) Math.round(doubleNumber / divider);
    return prettyNumber + LARGE_NUMBER_SUFFIXES[suffixIndex];
  }

  public static String getUniqueString() {
    return getUniqueString("");
  }

  public static String getUniqueString(String prefix) {
    return String.format("%s_%x_%x", prefix, System.nanoTime(), ThreadLocalRandom.current().nextInt());
  }

  public static String getUniqueTempPath() {
    return getUniqueTempPath("venice-tmp-");
  }

  public static String getUniqueTempPath(String prefix) {
    return Paths.get(FileUtils.getTempDirectoryPath(), getUniqueString(prefix)).toAbsolutePath().toString();
  }

  public static File getTempDataDirectory() {
    return getTempDataDirectory("venice-tmp-");
  }

  public static File getTempDataDirectory(String prefix) {
    try {
      File directory = new File(getUniqueTempPath(prefix));
      FileUtils.forceMkdir(directory);
      FileUtils.forceDeleteOnExit(directory);
      return directory;
    } catch (IOException e) {
      throw new VeniceException(e);
    }
  }

  private static class TimeUnitInfo {
    String suffix;
    int multiplier;
    DecimalFormat format;

    public TimeUnitInfo(String suffix, int multiplier, DecimalFormat format) {
      this.suffix = suffix;
      this.multiplier = multiplier;
      this.format = format;
    }
  }

  private static final Pair<String, Integer>[] TIME_SUFFIX_AND_MULTIPLIER =
      Arrays
          .asList(
              new Pair<>("ns", 1),
              new Pair<>("us", Time.NS_PER_US),
              new Pair<>("ms", Time.US_PER_MS),
              new Pair<>("s", Time.MS_PER_SECOND),
              new Pair<>("m", Time.SECONDS_PER_MINUTE),
              new Pair<>("h", Time.MINUTES_PER_HOUR))
          .toArray(new Pair[6]);

  private static final TimeUnitInfo[] TIME_UNIT_INFO = { new TimeUnitInfo("ns", 1, new DecimalFormat("0")),
      new TimeUnitInfo("us", Time.NS_PER_US, new DecimalFormat("0")),
      new TimeUnitInfo("ms", Time.US_PER_MS, new DecimalFormat("0")),
      new TimeUnitInfo("s", Time.MS_PER_SECOND, new DecimalFormat("0.0")),
      new TimeUnitInfo("m", Time.SECONDS_PER_MINUTE, new DecimalFormat("0.0")),
      new TimeUnitInfo("h", Time.MINUTES_PER_HOUR, new DecimalFormat("0.0")), };

  public static String makeTimePretty(long nanoSecTime) {
    double formattedTime = nanoSecTime;
    int timeUnitIndex = 0;
    while (timeUnitIndex < TIME_UNIT_INFO.length - 1) {
      int nextMultiplier = TIME_UNIT_INFO[timeUnitIndex + 1].multiplier;
      if (formattedTime < nextMultiplier) {
        break;
      } else {
        formattedTime = formattedTime / (double) nextMultiplier;
        timeUnitIndex++;
      }
    }
    DecimalFormat df = TIME_UNIT_INFO[timeUnitIndex].format;
    return df.format(formattedTime) + TIME_UNIT_INFO[timeUnitIndex].suffix;
  }

  public static String getCurrentWorkingDirectory() {
    Path currentRelativePath = Paths.get("");
    return currentRelativePath.toAbsolutePath().toString();
  }

  /**
   * Note: this may fail in some JVM implementations.
   *
   * Lifted from: https://stackoverflow.com/a/7690178
   *
   * @return the pid of the current Java process, or "N/A" if unavailable
   */
  public static String getPid() {
    try {
      // something like '<pid>@<hostname>', at least in SUN / Oracle JVMs
      final String jvmName = ManagementFactory.getRuntimeMXBean().getName();
      final int index = jvmName.indexOf('@');

      if (index < 1) {
        LOGGER.warn("Failed to determine pid");
        return "NA";
      }

      return Long.toString(Long.parseLong(jvmName.substring(0, index)));
    } catch (Exception e) {
      LOGGER.warn("Failed to determine pid", e);
      return "NA";
    }
  }

  /**
   * This might not work when application is running inside application server like Jetty
   *
   * @return the version of the venice-common jar on the classpath, if available, or "N/A" otherwise.
   */
  public static String getVeniceVersionFromClassPath() {
    // The application class loader is no longer an instance of java.net.URLClassLoader in JDK 9+
    // So changing implementation to be compatible with JDK8 and JDK11 at the same time
    String classpath = System.getProperty("java.class.path");
    String[] entries = classpath.split(File.pathSeparator);
    String jarNamePrefixToLookFor = "venice-common-";

    for (int i = 0; i < entries.length; i++) {
      int indexOfJarName = entries[i].lastIndexOf(jarNamePrefixToLookFor);
      if (indexOfJarName > -1) {
        int substringStart = indexOfJarName + jarNamePrefixToLookFor.length();
        return entries[i].substring(substringStart).replace(".jar", "");
      }
    }

    LOGGER.warn("Failed to determine Venice version");
    return "N/A";
  }

  public static String getCurrentUser() {
    try {
      return System.getProperty("user.name");
    } catch (Exception e) {
      LOGGER.warn("Failed to determine current user");
      return "N/A";
    }
  }

  public static int getJavaMajorVersion() {
    String[] versionElements = System.getProperty("java.version").split("\\.");
    int discard = Integer.parseInt(versionElements[0]);
    int version;
    if (discard == 1) {
      version = Integer.parseInt(versionElements[1]);
    } else {
      version = discard;
    }

    return version;
  }

  public static Map<CharSequence, CharSequence> getDebugInfo() {
    Map<CharSequence, CharSequence> debugInfo = new HashMap<>();
    try {
      debugInfo.put("host", getHostName());
    } catch (Exception e) {
      LOGGER.warn("Failed to determine host name");
      debugInfo.put("host", "N/A");
    }
    debugInfo.put("path", getCurrentWorkingDirectory());
    debugInfo.put("pid", getPid());
    debugInfo.put("version", getVeniceVersionFromClassPath());
    debugInfo.put("user", getCurrentUser());
    debugInfo.put("JDK major version", Integer.toString(getJavaMajorVersion()));

    return debugInfo;
  }

  /**
   * This class encapsulates config entity information such as config name, default value, config document
   * @param <T> Type of the default value to this config properties
   */
  public static class ConfigEntity<T> {
    private final String configName;
    private final T defaultValue;
    private final String doc;

    public ConfigEntity(@Nonnull String configName) {
      this(configName, null, null);
    }

    public ConfigEntity(@Nonnull String configName, @Nullable T defaultValue) {
      this(configName, defaultValue, null);
    }

    public ConfigEntity(@Nonnull String configName, @Nullable T defaultValue, @Nullable String doc) {
      Validate.notEmpty(configName);
      this.configName = configName;
      this.defaultValue = defaultValue;
      this.doc = doc;
    }

    @Nonnull
    public String getConfigName() {
      return configName;
    }

    @Nullable
    public T getDefaultValue() {
      return defaultValue;
    }

    @Nullable
    public String getDoc() {
      return doc;
    }
  }

  /**
   * Given a key with type {@link String} and get a value (if there is one) from a map of type {@link Map<CharSequence, CharSequence>}
   * which has one of key's toString value equals to the given String key. Note that the worst case runtime of this function
   * is O(n) since the given map is iterated to find the value.
   *
   * The reason why this function exists is that the specific implementation of the {@link CharSequence} interface might
   * not be {@link String} for the map key type. So, when users try to get a value from such a map with a String key, they
   * might not get the value even when the value of the key which has the same toString representation as the user-given
   * String key since the specific implementation of the {@link CharSequence} interface has a different hash function than
   * String's hash function.
   *
   * @return a Value in the CharSequence map or {@link null}
   */
  public static Optional<CharSequence> getValueFromCharSequenceMapWithStringKey(
      Map<CharSequence, CharSequence> charSequenceMap,
      String stringKey) {
    for (Map.Entry<CharSequence, CharSequence> entry: charSequenceMap.entrySet()) {
      if (Objects.equals(entry.getKey().toString(), stringKey)) {
        return Optional.ofNullable(entry.getValue()); // Value can be null
      }
    }
    return Optional.empty();
  }

  // TODO (lcli): Remove it when Java 9+ is used since Set.of is supported in Java 9+
  @SafeVarargs
  public static <T> Set<T> setOf(T... objs) {
    return Collections.unmodifiableSet(new HashSet<>(Arrays.asList(objs)));
  }

  @SafeVarargs
  public static <T> Set<T> mutableSetOf(T... objs) {
    return new HashSet<>(Arrays.asList(objs));
  }

  public static long calculateDurationMs(Time time, long startTimeMs) {
    return time.getMilliseconds() - startTimeMs;
  }

  public static void closeQuietlyWithErrorLogged(Closeable... closeables) {
    if (closeables == null) {
      return;
    }
    for (Closeable closeable: closeables) {
      IOUtils.closeQuietly(closeable, LOGGER::error);
    }
  }

  public static List<Replica> getReplicasForInstance(RoutingDataRepository routingDataRepo, String instanceId) {
    ResourceAssignment resourceAssignment = routingDataRepo.getResourceAssignment();
    List<Replica> replicas = new ArrayList<>();
    // lock resource assignment to avoid it's updated by routing data repository during the searching.
    synchronized (resourceAssignment) {
      Set<String> resourceNames = resourceAssignment.getAssignedResources();

      for (String resourceName: resourceNames) {
        PartitionAssignment partitionAssignment = resourceAssignment.getPartitionAssignment(resourceName);
        for (Partition partition: partitionAssignment.getAllPartitions()) {
          HelixState helixState = partition.getHelixStateByInstanceId(instanceId);
          ExecutionStatus executionStatus = partition.getExecutionStatusByInstanceId(instanceId);
          if (helixState != null || executionStatus != null) {
            /**
             * N.B.: We only add the {@link Replica} to the returned list if the partition is hosted on the provided
             * {@param instanceId}, which we consider to be the case if the {@link Partition} object carries either
             * an {@link ExecutionStatus} and/or a {@link HelixState} for it.
             */
            Replica replica = new Replica(Instance.fromNodeId(instanceId), partition.getId(), resourceName);
            replica.setStatus(helixState);
            replicas.add(replica);
          }
        }
      }
    }
    return replicas;
  }

  public static boolean isCurrentVersion(String resourceName, ReadOnlyStoreRepository metadataRepo) {
    try {
      String storeName = Version.parseStoreFromKafkaTopicName(resourceName);
      int version = Version.parseVersionFromKafkaTopicName(resourceName);

      Store store = metadataRepo.getStore(storeName);
      if (store != null) {
        return store.getCurrentVersion() == version;
      } else {
        LOGGER.error("Store {} is not in store repository.", storeName);
        // If a store doesn't exist, it doesn't have current version
        return false;
      }
    } catch (VeniceException e) {
      return false;
    }
  }

  public static boolean isFutureVersion(String resourceName, ReadOnlyStoreRepository metadataRepo) {
    try {
      String storeName = Version.parseStoreFromKafkaTopicName(resourceName);
      int version = Version.parseVersionFromKafkaTopicName(resourceName);

      Store store = metadataRepo.getStore(storeName);
      if (store == null) {
        LOGGER.warn("Store {} is not in store repository.", storeName);
        // If a store doesn't exist, it is not a future version
        return false;
      }
      return store.getCurrentVersion() < version;
    } catch (VeniceException e) {
      return false;
    }
  }

  /**
   * When Helix thinks some host is overloaded or a new host joins the cluster, it might move some replicas from
   * one host to another. The partition to be moved is usually in a healthy state, i.e. 3/3 running replicas.
   * We will now build an extra replica in the new host before dropping one replica in an old host.
   * i.e. replica changes 3 -> 4 -> 3. In this scenario, we may see an extra replica appear.
   *
   * This function determines if a replica is an extra replica.
   * @return true, if the input replica is an extra one.
   *         false, otherwise.
   */
  public static boolean isExtraReplica(
      ReadOnlyStoreRepository metadataRepo,
      Replica replica,
      List<Instance> readyInstances) {
    String storeName = Version.parseStoreFromKafkaTopicName(replica.getResource());
    Store store = metadataRepo.getStore(storeName);

    return readyInstances.contains(replica.getInstance())
        ? readyInstances.size() > store.getReplicationFactor()
        : readyInstances.size() >= store.getReplicationFactor();
  }

  public static Map<String, String> extractQueryParamsFromRequest(
      Map<String, String[]> sparkRequestParams,
      ControllerResponse response) {
    boolean anyParamContainsMoreThanOneValue =
        sparkRequestParams.values().stream().anyMatch(strings -> strings.length > 1);

    if (anyParamContainsMoreThanOneValue) {
      VeniceException e = new VeniceException(
          "Array parameters are not supported. Provided request parameters: " + sparkRequestParams,
          ErrorType.BAD_REQUEST);
      response.setError(e);
      throw e;
    }

    Map<String, String> params = sparkRequestParams.entrySet()
        .stream()
        // Extract the first (and only) value of each param
        .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()[0]));
    return params;
  }

  public static Pair<Store, Version> waitStoreVersionOrThrow(
      String storeVersionName,
      ReadOnlyStoreRepository metadataRepo) {
    String storeName = Version.parseStoreFromKafkaTopicName(storeVersionName);
    int versionNumber = Version.parseVersionFromKafkaTopicName(storeVersionName);

    Pair<Store, Version> storeVersionPair = metadataRepo.waitVersion(storeName, versionNumber, Duration.ofSeconds(30));
    if (storeVersionPair.getFirst() == null) {
      throw new VeniceException("Store " + storeName + " does not exist.");
    }
    if (storeVersionPair.getSecond() == null) {
      throw new VeniceException("Store " + storeName + " version " + versionNumber + " does not exist.");
    }
    return storeVersionPair;
  }

  public static <K, V> Iterator<V> iterateOnMapOfLists(Map<K, List<V>> mapOfLists) {
    return new Iterator<V>() {
      private Iterator<Map.Entry<K, List<V>>> mapIterator = mapOfLists.entrySet().iterator();
      private Iterator<V> listIterator = Collections.emptyIterator();

      @Override
      public boolean hasNext() {
        while (!listIterator.hasNext() && mapIterator.hasNext()) {
          listIterator = mapIterator.next().getValue().iterator();
        }
        return listIterator.hasNext();
      }

      @Override
      public V next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return listIterator.next();
      }
    };
  }
}
