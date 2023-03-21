package com.linkedin.venice.datarecovery;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * StoreRepushCommand contains the details of executing/processing repush command.
 * We expect the command to comply with the following contract:
 *
 * Input:
 *    <COMMAND> [<EXTRA_COMMAND_ARGS>] --store <store_name> --fabric <source_fabric>
 * Output:
 *    success: link_to_running_task
 *    failure: failure_reason
 */

public class StoreRepushCommand {
  private static final Logger LOGGER = LogManager.getLogger(StoreRepushCommand.class);

  // Store name.
  private String store;
  private Params params;
  private Result result;
  private List<String> shellCmd;

  // For unit test only.
  public StoreRepushCommand() {
  }

  public StoreRepushCommand(String store, Params params) {
    this.store = store;
    this.params = params;
    this.shellCmd = generateShellCmd();
  }

  // For unit test only.
  public void setParams(Params params) {
    this.params = params;
  }

  public String getStore() {
    return store;
  }

  public Result getResult() {
    return result;
  }

  private List<String> generateRepushCommand() {
    List<String> cmd = new ArrayList<>();
    cmd.add(this.params.command);
    cmd.add(this.params.extraCommandArgs);
    cmd.add(String.format("--store '%s'", store));
    cmd.add(String.format("--fabric '%s'", this.params.sourceFabric));
    return cmd;
  }

  private List<String> generateShellCmd() {
    List<String> shellCmd = new ArrayList<>();
    // Start a shell process so that it contains the right PATH variables.
    shellCmd.add("sh");
    shellCmd.add("-c");
    shellCmd.add(String.join(" ", generateRepushCommand()));
    return shellCmd;
  }

  public List<String> getShellCmd() {
    if (shellCmd == null) {
      shellCmd = generateShellCmd();
    }
    return shellCmd;
  }

  public void processOutput(String output, int exitCode) {
    result = new Result();
    result.setStdOut(output);
    result.setExitCode(exitCode);
    result.parseStandardOutput();
  }

  public void execute() {
    ProcessBuilder pb = new ProcessBuilder(getShellCmd());
    // so we can ignore the error stream.
    pb.redirectErrorStream(true);
    int exitCode = -1;
    BufferedReader reader = null;
    String stdOut = StringUtils.EMPTY;
    try {
      Process process = pb.start();
      reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      String line;
      StringBuilder buf = new StringBuilder();
      while ((line = reader.readLine()) != null) {
        buf.append(line);
        buf.append('\n');
      }
      stdOut = buf.toString();
      // remove trailing white spaces and new lines.
      stdOut = stdOut.trim();

      exitCode = process.waitFor();
      processOutput(stdOut, exitCode);
    } catch (IOException e) {
      LOGGER.error("Error in executing command: {}", this, e);
    } catch (InterruptedException e) {
      LOGGER.warn("Interrupted when waiting for executing command: {}", this, e);
    } finally {
      try {
        if (reader != null) {
          reader.close();
        }
      } catch (IOException e) {
        LOGGER.error("Error in closing reader for command: {}", this, e);
      }
    }

    if (params.debug) {
      LOGGER.info("Cmd: {}, StdOut: {}, Exit code: {}", this, stdOut, exitCode);
    }
  }

  @Override
  public String toString() {
    return "StoreRepushCommand{\n" + String.join(" ", shellCmd) + "\n}";
  }

  public static class Params {
    // command name.
    private String command;
    // source fabric.
    private String sourceFabric;
    // extra arguments to command.
    private String extraCommandArgs;
    // Debug run.
    private boolean debug = false;

    public void setDebug(boolean debug) {
      this.debug = debug;
    }

    public void setCommand(String cmd) {
      this.command = cmd;
    }

    public void setExtraCommandArgs(String args) {
      this.extraCommandArgs = args;
    }

    public void setSourceFabric(String fabric) {
      this.sourceFabric = fabric;
    }
  }

  public static class Result {
    private String cluster;
    private String store;
    private String stdOut;
    private int exitCode;
    private String error;
    private String message;

    public int getExitCode() {
      return exitCode;
    }

    public void setExitCode(int exitCode) {
      this.exitCode = exitCode;
    }

    public String getStdOut() {
      return stdOut;
    }

    public void setStdOut(String stdOut) {
      this.stdOut = stdOut;
    }

    public boolean isError() {
      return error != null;
    }

    public String getCluster() {
      return cluster;
    }

    public void setCluster(String cluster) {
      this.cluster = cluster;
    }

    public String getStore() {
      return store;
    }

    public void setStore(String store) {
      this.store = store;
    }

    public void setError(String error) {
      this.error = error;
    }

    public String getError() {
      return error;
    }

    public String getMessage() {
      return message;
    }

    public void setMessage(String message) {
      this.message = message;
    }

    public void parseStandardOutput() {
      // No standard output or empty output.
      if (stdOut == null || stdOut.equals(StringUtils.EMPTY)) {
        return;
      }

      // Command reached to Azkaban, no matter it was a success or a failure.
      if (matchSuccessPattern()) {
        return;
      }

      if (matchFailurePattern()) {
        return;
      }

      // Failed: repush command itself hit an error (e.g. incomplete parameters)
      error = stdOut;
    }

    private boolean matchSuccessPattern() {
      // success: https://example.com/executor?execid=21585379
      String successPattern = "^success: (.*)$";
      Pattern pattern = Pattern.compile(successPattern, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
      Matcher matcher = pattern.matcher(stdOut);
      if (matcher.find()) {
        message = matcher.group();
        return true;
      }
      return false;
    }

    private boolean matchFailurePattern() {
      // failure: invalid password
      String errorPattern = "^failure: (.*)$";
      Pattern pattern = Pattern.compile(errorPattern, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
      Matcher matcher = pattern.matcher(stdOut);
      // Find the first occurrence of an error line and report.
      if (matcher.find()) {
        error = matcher.group();
        return true;
      }
      return false;
    }
  }
}
