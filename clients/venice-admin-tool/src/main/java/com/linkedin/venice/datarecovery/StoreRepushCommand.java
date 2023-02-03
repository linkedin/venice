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
 */

public class StoreRepushCommand {
  private static final Logger LOGGER = LogManager.getLogger(StoreRepushCommand.class);
  private static final String VENICE_TOOLS = "venice-tools";
  private static final String REPUSH_MODULE = "repush";
  private static final String REPUSH_SOURCE_KAFKA = "kafka";

  // Store name.
  private String store;
  private Params params;
  private Result result;
  private List<String> expectCmd;

  public StoreRepushCommand() {
  }

  public StoreRepushCommand(String store, Params params) {
    this.store = store;
    this.params = params;
    this.expectCmd = generateExpectCmd();
  }

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
    cmd.add(VENICE_TOOLS);
    cmd.add(REPUSH_MODULE);
    cmd.add(REPUSH_SOURCE_KAFKA);

    if (store != null) {
      cmd.add("--store " + store);
    }

    if (params.fabric != null) {
      cmd.add("--fabric " + params.fabric);
    }

    if (params.rewindTimeOverrideSeconds != -1) {
      cmd.add("--rewind_time_override_seconds " + params.rewindTimeOverrideSeconds);
    }

    if (params.fabricGroup != null) {
      cmd.add("--fabric_group " + params.fabricGroup);
    }

    if (params.force) {
      cmd.add("--force");
    }

    if (params.loginUser != null) {
      cmd.add("--login_user " + params.loginUser);
    }

    if (params.proxyUser != null) {
      cmd.add("--proxy_user " + params.proxyUser);
    }

    if (params.storelistFile != null) {
      cmd.add("--storelist_file " + params.storelistFile);
    }

    if (params.jobConcurrency != -1) {
      cmd.add("--job_concurrency " + params.jobConcurrency);
    }
    return cmd;
  }

  private List<String> generateExpectCmd() {
    List<String> expectCmd = new ArrayList<>();
    // Start a shell process so that it contains the right PATH variables.
    expectCmd.add("sh");
    expectCmd.add("-c");
    /*
     * repush.py uses getpass.getpass to prompt the user for a password without echoing. It adds difficulties in handling
     * its input/ouput as by defaults it always reads and writes directly on the tty device i.e. its controlling terminal
     * (/dev/tty) thus bypassing the stdin, stdout, or stderr.
     *
     * We use expect tool to solve this issue because expect tool uses pseudo-terminals (ptys). Ptys are logical device
     * drivers that gives the program an illusion that it is a real terminal drivers. Programs that open /dev/tty will
     * actually end up speaking to their pty.
     *
     * The following commands:
     * 1. creates a new process running repush command.
     * 2. waits until the output of the process to match a pattern (Password + VIP:) from the terminal then send the
     *    pre-defined password to the process.
     * 3. exits the process if no such pattern is discovered from the terminal.
     * 4. waits until the successful or failed messages from Azkaban.
     */
    expectCmd.add(
        "expect -c \"spawn -noecho " + String.join(" ", generateRepushCommand()) + "\n" + "expect {\n"
            + "{*Password + VIP:*} {send " + this.params.getPassword() + "\\r}\n"
            + "-re \"^\\(\\(?!Password\\).\\)+\\$\" {exit}\n" + "}\n" + "expect {*https*} {}\n" + "\"");
    return expectCmd;
  }

  public List<String> getExpectCmd() {
    if (expectCmd == null) {
      expectCmd = generateExpectCmd();
    }
    return expectCmd;
  }

  public void processOutput(String output, int exitCode) {
    result = new Result();
    result.setStdOut(output);
    result.setExitCode(exitCode);
    result.parseStandardOutput();
  }

  public void execute() {
    ProcessBuilder pb = new ProcessBuilder(getExpectCmd());
    // so we can ignore the error stream.
    pb.redirectErrorStream(true);
    int exitCode = -1;
    BufferedReader reader = null;
    String stdOut = StringUtils.EMPTY;
    try {
      Process process = pb.start();
      reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      String line;
      StringBuffer buf = new StringBuffer();
      while ((line = reader.readLine()) != null) {
        buf.append(line + "\n");
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
    String cmd = "StoreRepushCommand{\n" + String.join(" ", expectCmd) + "\n}";
    // Hide user's password.
    cmd = cmd.replace(params.getPassword(), "******");
    return cmd;
  }

  public static class Params {
    // Fabric to recover data from, must be used with --force.
    private String fabric;
    // Rewind time to trigger the store with.
    private long rewindTimeOverrideSeconds = -1;
    // Fabric group from which the largest version will be chosen for repush.
    private String fabricGroup;
    // Must be used when specifying --fabric option to repush.
    private boolean force = false;
    // User that interacts with Azkaban.
    private String loginUser;
    // Proxy user to run the job.
    private String proxyUser;
    // Password + VIP.
    private String password;
    // Debug run.
    private boolean debug = false;
    /*
     * Filename with a list of stores (one per line) to repush to. Lines beginning with # are ignored.
     * Required if --job_concurrency is specified.
     */
    private String storelistFile;
    // Number of repush jobs to run concurrently. Maximum: 50 (default: 10).
    private int jobConcurrency = -1;

    public void setFabric(String fabric) {
      this.fabric = fabric;
    }

    public void setRewindTimeOverrideSeconds(long rewindTimeOverrideSeconds) {
      this.rewindTimeOverrideSeconds = rewindTimeOverrideSeconds;
    }

    public void setFabricGroup(String fabricGroup) {
      this.fabricGroup = fabricGroup;
    }

    public void setForce(boolean force) {
      this.force = force;
    }

    public void setLoginUser(String loginUser) {
      this.loginUser = loginUser;
    }

    public void setProxyUser(String proxyUser) {
      this.proxyUser = proxyUser;
    }

    public void setStorelistFile(String storelistFile) {
      this.storelistFile = storelistFile;
    }

    public void setJobConcurrency(int jobConcurrency) {
      this.jobConcurrency = jobConcurrency;
    }

    public void setPassword(String password) {
      this.password = password;
    }

    public String getPassword() {
      return this.password;
    }

    public void setDebug(boolean debug) {
      this.debug = debug;
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
      if (matchAzkabanSuccessPattern()) {
        return;
      }

      if (matchAzkabanFailurePattern()) {
        return;
      }

      // Failed: repush command itself hit an error (e.g. incomplete parameters)
      error = stdOut;
    }

    private boolean matchAzkabanSuccessPattern() {
      // Success: (example) https://ltx1-faroaz01.grid.linkedin.com:8443/executor?execid=21585379
      String successPattern = "^https(.*)execid(.*)$";
      Pattern pattern = Pattern.compile(successPattern, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
      Matcher matcher = pattern.matcher(stdOut);
      if (matcher.find()) {
        message = matcher.group();
        return true;
      }
      return false;
    }

    private boolean matchAzkabanFailurePattern() {
      // Failed: Handle azkaban failed response to check if the output line contains any errors. (e.g. invalid password)
      String errorPattern = "^(.*)Response(.*)error(.*)$";
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
