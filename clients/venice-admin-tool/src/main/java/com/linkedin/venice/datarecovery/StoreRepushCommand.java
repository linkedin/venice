package com.linkedin.venice.datarecovery;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.MultiStoreStatusResponse;
import com.linkedin.venice.controllerapi.StoreHealthAuditResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.datarecovery.meta.RepushViabilityInfo;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.RegionPushDetails;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.security.SSLFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

public class StoreRepushCommand extends Command {
  private static final Logger LOGGER = LogManager.getLogger(StoreRepushCommand.class);

  private Params params;
  private Result result = new Result();
  private RepushViabilityInfo.Result repushViabilityResult = RepushViabilityInfo.Result.NOT_STARTED;
  private List<String> shellCmd;

  private boolean isShellCmdExecuted = false;

  // For unit test only.
  public StoreRepushCommand() {
  }

  public StoreRepushCommand(Params params) {
    this.params = params;
    this.shellCmd = generateShellCmd();
  }

  // For unit test only.
  public void setParams(Params params) {
    this.params = params;
  }

  public StoreRepushCommand.Params getParams() {
    return this.params;
  }

  @Override
  public StoreRepushCommand.Result getResult() {
    return result;
  }

  @Override
  public boolean needWaitForFirstTaskToComplete() {
    return true;
  }

  public boolean isShellCmdExecuted() {
    return isShellCmdExecuted;
  }

  public void completeCoreWorkWithMessage(String message) {
    getResult().setMessage(message);
    getResult().setCoreWorkDone(true);
  }

  public void completeCoreWorkWithError(String error) {
    getResult().setError(error);
    getResult().setCoreWorkDone(true);
  }

  private List<String> generateRepushCommand() {
    List<String> cmd = new ArrayList<>();
    cmd.add(this.getParams().command);
    cmd.add(this.getParams().extraCommandArgs);
    cmd.add(String.format("--store '%s'", this.getParams().store));
    cmd.add(String.format("--fabric '%s'", this.getParams().sourceFabric));
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

  public RepushViabilityInfo.Result getViabilityResult() {
    return repushViabilityResult;
  }

  private void processOutput(String output, int exitCode) {
    getResult().setStdOut(output);
    getResult().setExitCode(exitCode);
    getResult().parseStandardOutput();
    getResult().setCoreWorkDone(true);
  }

  public RepushViabilityInfo getRepushViability() {
    String url = getParams().getUrl();
    ControllerClient cli = getParams().getPCtrlCliWithoutCluster();
    LocalDateTime timestamp = getParams().getTimestamp();
    String destFabric = getParams().getDestFabric();
    String s = getParams().getStore();
    RepushViabilityInfo ret = new RepushViabilityInfo();
    try {
      String clusterName = cli.discoverCluster(s).getCluster();
      if (clusterName == null) {
        return ret.inViableWithError(RepushViabilityInfo.Result.DISCOVERY_ERROR);
      }
      try (ControllerClient parentCtrlCli = buildControllerClient(clusterName, url, getParams().getSSLFactory())) {
        StoreResponse storeResponse = parentCtrlCli.getStore(s);
        if (storeResponse.getStore().getHybridStoreConfig() != null) {
          ret.setHybrid(true);
        }

        StoreHealthAuditResponse storeHealthInfo = parentCtrlCli.listStorePushInfo(s, false);
        Map<String, RegionPushDetails> regionPushDetails = storeHealthInfo.getRegionPushDetails();
        if (!regionPushDetails.containsKey(destFabric)) {
          return ret.inViableWithError(RepushViabilityInfo.Result.NO_CURRENT_VERSION);
        }
        String latestTimestamp = regionPushDetails.get(destFabric).getPushStartTimestamp();
        LocalDateTime latestPushStartTime = LocalDateTime.parse(latestTimestamp, DateTimeFormatter.ISO_LOCAL_DATE_TIME);

        if (latestPushStartTime.isAfter(timestamp)) {
          return ret.inViableWithResult(RepushViabilityInfo.Result.CURRENT_VERSION_IS_NEWER);
        }

        MultiStoreStatusResponse response = parentCtrlCli.getFutureVersions(clusterName, s);
        // No future version status for target region.
        if (!response.getStoreStatusMap().containsKey(destFabric)) {
          return ret.viableWithResult(RepushViabilityInfo.Result.SUCCESS);
        }

        int futureVersion = Integer.parseInt(response.getStoreStatusMap().get(destFabric));
        // No ongoing offline pushes detected for target region.
        if (futureVersion == Store.NON_EXISTING_VERSION) {
          return ret.viableWithResult(RepushViabilityInfo.Result.SUCCESS);
        }
        // Find ongoing pushes for this store, skip.
        return ret.inViableWithResult(RepushViabilityInfo.Result.ONGOING_PUSH);
      }
    } catch (VeniceException e) {
      return ret.inViableWithError(RepushViabilityInfo.Result.EXCEPTION_THROWN);
    }
  }

  @Override
  public void execute() {
    RepushViabilityInfo repushViability = getRepushViability();
    this.repushViabilityResult = repushViability.getResult();
    StoreRepushCommand.Params repushParams = getParams();
    ControllerClient cli = repushParams.getPCtrlCliWithoutCluster();
    if (!repushViability.isViable()) {
      if (repushViability.isError()) {
        completeCoreWorkWithError(repushViability.getResult().toString());
      } else {
        completeCoreWorkWithMessage(repushViability.getResult().toString());
      }
      return;
    }

    if (!repushViability.isHybrid()) {
      try {
        String clusterName = cli.discoverCluster(repushParams.getStore()).getCluster();
        try (ControllerClient parentCtrlCli =
            buildControllerClient(clusterName, repushParams.getUrl(), repushParams.getSSLFactory())) {
          ControllerResponse prepareResponse = parentCtrlCli.prepareDataRecovery(
              repushParams.getSourceFabric(),
              repushParams.getDestFabric(),
              repushParams.getStore(),
              -1,
              Optional.empty());
          if (prepareResponse.isError()) {
            completeCoreWorkWithError(prepareResponse.getError());
            return;
          }
          ControllerResponse dataRecoveryResponse = parentCtrlCli.dataRecovery(
              repushParams.getSourceFabric(),
              repushParams.getDestFabric(),
              repushParams.getStore(),
              -1,
              false,
              true,
              Optional.empty());
          if (dataRecoveryResponse.isError()) {
            completeCoreWorkWithError(dataRecoveryResponse.getError());
            return;
          }
          completeCoreWorkWithMessage("batch store with data recovery API -- no url");
        }
      } catch (VeniceException e) {
        completeCoreWorkWithError(e.getMessage());
      }
      return;
    }
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
      isShellCmdExecuted = true;
      try {
        if (reader != null) {
          reader.close();
        }
      } catch (IOException e) {
        LOGGER.error("Error in closing reader for command: {}", this, e);
      }
    }

    if (getParams().debug) {
      LOGGER.info("Cmd: {}, StdOut: {}, Exit code: {}", this, stdOut, exitCode);
    }
  }

  @Override
  public String toString() {
    return "StoreRepushCommand{\n" + String.join(" ", shellCmd) + "\n}";
  }

  public static class Params extends Command.Params {
    // command name.
    private String command;
    // dest fabric
    private String destFabric;
    // source fabric.
    private String sourceFabric;
    // extra arguments to command.
    private String extraCommandArgs;
    // expected completion timestamp
    private LocalDateTime timestamp;
    // Debug run.
    private boolean debug = false;

    private ControllerClient pCtrlCliWithoutCluster;
    private String url;
    private Optional<SSLFactory> sslFactory;

    public String getCommand() {
      return this.command;
    }

    public String getUrl() {
      return url;
    }

    public ControllerClient getPCtrlCliWithoutCluster() {
      return this.pCtrlCliWithoutCluster;
    }

    public LocalDateTime getTimestamp() {
      return this.timestamp;
    }

    public String getDestFabric() {
      return this.destFabric;
    }

    public Optional<SSLFactory> getSSLFactory() {
      return sslFactory;
    }

    public String getSourceFabric() {
      return sourceFabric;
    }

    public boolean getDebug() {
      return this.debug;
    }

    public static class Builder {
      private String command;
      private String destFabric;
      private String sourceFabric;
      private String extraCommandArgs;
      private LocalDateTime timestamp;
      private ControllerClient pCtrlCliWithoutCluster;
      private String url;
      private Optional<SSLFactory> sslFactory;
      private boolean debug = false;

      public Builder() {
      }

      public Builder(
          String command,
          String destFabric,
          String sourceFabric,
          String extraCommandArgs,
          LocalDateTime timestamp,
          ControllerClient controllerClient,
          String url,
          Optional<SSLFactory> sslFactory,
          boolean debug) {
        this.setCommand(command)
            .setDestFabric(destFabric)
            .setSourceFabric(sourceFabric)
            .setExtraCommandArgs(extraCommandArgs)
            .setTimestamp(timestamp)
            .setPCtrlCliWithoutCluster(controllerClient)
            .setUrl(url)
            .setSSLFactory(sslFactory)
            .setDebug(debug);
      }

      public Builder(StoreRepushCommand.Params p) {
        this(
            p.command,
            p.destFabric,
            p.sourceFabric,
            p.extraCommandArgs,
            p.timestamp,
            p.pCtrlCliWithoutCluster,
            p.url,
            p.sslFactory,
            p.debug);
      }

      public StoreRepushCommand.Params build() {
        StoreRepushCommand.Params ret = new StoreRepushCommand.Params();
        ret.command = command;
        ret.destFabric = destFabric;
        ret.sourceFabric = sourceFabric;
        ret.extraCommandArgs = extraCommandArgs;
        ret.timestamp = timestamp;
        ret.pCtrlCliWithoutCluster = pCtrlCliWithoutCluster;
        ret.url = url;
        ret.sslFactory = sslFactory;
        ret.debug = debug;
        return ret;
      }

      public StoreRepushCommand.Params.Builder setCommand(String command) {
        this.command = command;
        return this;
      }

      public StoreRepushCommand.Params.Builder setDestFabric(String destFabric) {
        this.destFabric = destFabric;
        return this;
      }

      public StoreRepushCommand.Params.Builder setSourceFabric(String sourceFabric) {
        this.sourceFabric = sourceFabric;
        return this;
      }

      public StoreRepushCommand.Params.Builder setExtraCommandArgs(String extraCommandArgs) {
        this.extraCommandArgs = extraCommandArgs;
        return this;
      }

      public StoreRepushCommand.Params.Builder setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
        return this;
      }

      public StoreRepushCommand.Params.Builder setPCtrlCliWithoutCluster(ControllerClient pCtrlCliWithoutCluster) {
        this.pCtrlCliWithoutCluster = pCtrlCliWithoutCluster;
        return this;
      }

      public StoreRepushCommand.Params.Builder setUrl(String url) {
        this.url = url;
        return this;
      }

      public StoreRepushCommand.Params.Builder setSSLFactory(Optional<SSLFactory> sslFactory) {
        this.sslFactory = sslFactory;
        return this;
      }

      public StoreRepushCommand.Params.Builder setDebug(boolean debug) {
        this.debug = debug;
        return this;
      }
    }
  }

  public static class Result extends Command.Result {
    private String stdOut = null;
    private int exitCode;

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
