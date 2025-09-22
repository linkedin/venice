package com.linkedin.venice.duckdb;

import com.linkedin.venice.utils.CliUtils;


public enum Arg implements CliUtils.CliArg {
  STORE_NAME("store-name", "sn", true, "The name of the Venice store you want to ingest from"),
  ZK_HOST_URL("zk-host-url", "zkurl", true, "The zk host url used by D2"),
  DUCKDB_OUTPUT_DIRECTORY(
      "duckdb-output-directory", "outputdir", true, "The directory of where you want your DuckDB file to be written to"
  ),
  COLUMNS_TO_PROJECT(
      "columns-to-project", "ctp", true, "Comma-separated list of columns to project from the store data"
  ),
  // Common CLI options
  DISABLE_LOG("disable-log", "dl", false, "Disable logs from internal classes. Only print command output on console"),
  FLAT_JSON("flat-json", "flj", false, "Display output as flat json, without pretty-print indentation and line breaks"),
  HELP("help", "h", false, "Show usage"), SSL_CONFIG_PATH("ssl-config-path", "scp", true, "SSL config file path");

  private final String argName;
  private final String first;
  private final boolean parameterized;
  private final String helpText;

  Arg(String argName, String first, boolean parameterized, String helpText) {
    this.argName = argName;
    this.first = first;
    this.parameterized = parameterized;
    this.helpText = helpText;
  }

  @Override
  public String toString() {
    return argName;
  }

  public String first() {
    return first;
  }

  public String getArgName() {
    return argName;
  }

  public String getHelpText() {
    return helpText;
  }

  public boolean isParameterized() {
    return parameterized;
  }
}
