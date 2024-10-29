package com.linkedin.venice.proxyjob.datawriter.utils;

import com.linkedin.venice.compression.GzipCompressor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.PushJobSetting;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.EncodingUtils;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.SerializationUtils;


public final class ProxyJobCliUtils {
  private ProxyJobCliUtils() {
  }

  private enum Arg {
    PROPS("props", "p", true, "Serialized job properties"),
    PUSH_JOB_SETTINGS("push-job-settings", "s", true, "Serialized push job settings"),
    OUTPUT_DIR("output-dir", "o", true, "Path of the directory where the job should output it's final state");

    private final Option option;

    Arg(String longOpt, String shortOpt, boolean parameterized, String helpText) {
      this.option =
          Option.builder(shortOpt).longOpt(longOpt).argName(longOpt).hasArg(parameterized).desc(helpText).build();
    }

    public Option getOption() {
      return this.option;
    }

    public String getShortOpt() {
      return this.option.getOpt();
    }

    public String getName() {
      return this.option.getArgName();
    }

    public String toString() {
      return this.getName();
    }
  }

  public static List<String> toCliArgs(Properties properties, PushJobSetting pushJobSetting, String outputDir) {
    List<String> cliArgs = new ArrayList<>();
    addComplexCliArgs(cliArgs, Arg.PROPS, properties);
    addComplexCliArgs(cliArgs, Arg.PUSH_JOB_SETTINGS, pushJobSetting);
    addCliArg(cliArgs, Arg.OUTPUT_DIR, outputDir);
    return cliArgs;
  }

  private static void addComplexCliArgs(List<String> cliArgs, Arg arg, Serializable serializableObj) {
    try (GzipCompressor compressor = new GzipCompressor()) {
      byte[] data = compressor.compress(SerializationUtils.serialize(serializableObj));
      String dataB64 = EncodingUtils.base64EncodeToString(data);
      addCliArg(cliArgs, arg, dataB64);
    } catch (IOException e) {
      throw new VeniceException(e);
    }
  }

  private static void addCliArg(List<String> cliArgs, Arg key, String value) {
    if (value == null) {
      return;
    }

    cliArgs.add("-" + key.getOption().getOpt());
    cliArgs.add(value);
  }

  public static CommandLine parseArgs(String[] args) throws ParseException {
    Options options = new Options();
    for (Arg option: Arg.values()) {
      options.addOption(option.getOption());
    }

    CommandLineParser parser = new DefaultParser(false);
    return parser.parse(options, args);
  }

  private static <T extends Serializable> T getComplexObjectFromCliArgs(CommandLine commandLine, Arg arg) {
    String dataStr = commandLine.getOptionValue(arg.getShortOpt());
    try (GzipCompressor compressor = new GzipCompressor()) {
      byte[] compressed = EncodingUtils.base64DecodeFromString(dataStr);
      byte[] data = ByteUtils.extractByteArray(compressor.decompress(compressed, 0, compressed.length));
      return (T) SerializationUtils.deserialize(data);
    } catch (IOException e) {
      throw new VeniceException(e);
    }
  }

  public static Properties getPropertiesFromCliArgs(CommandLine commandLine) {
    return getComplexObjectFromCliArgs(commandLine, Arg.PROPS);
  }

  public static PushJobSetting getPushJobSettingFromCliArgs(CommandLine commandLine) {
    return getComplexObjectFromCliArgs(commandLine, Arg.PUSH_JOB_SETTINGS);
  }

  public static String getOutputDirFromCliArgs(CommandLine commandLine) {
    return commandLine.getOptionValue(Arg.OUTPUT_DIR.getShortOpt());
  }
}
