package com.linkedin.venice.pushmonitor;

import static com.linkedin.venice.VeniceConstants.TYPE_PUSH_STATUS;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.routerapi.PushStatusResponse;
import com.linkedin.venice.samza.SamzaExitMode;
import com.linkedin.venice.samza.VeniceSystemFactory;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.Utils;
import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.samza.system.SystemProducer;


/**
 * This push monitor is able to query push job status from routers; it only works for
 * stores running in Leader/Follower mode and it will be built for STREAM_REPROCESSING job.
 */
public class RouterBasedPushMonitor implements Closeable {
  private static final Logger LOGGER = LogManager.getLogger(RouterBasedPushMonitor.class);

  private static final int POLL_CYCLE_DELAY_MS = 10000;
  private static final long POLL_TIMEOUT_MS = 10000L;

  private final String topicName;
  private final ExecutorService executor;

  private final PushMonitorTask pushMonitorTask;
  private ExecutionStatus currentStatus = ExecutionStatus.UNKNOWN;

  public RouterBasedPushMonitor(
      TransportClient d2TransportClient,
      String resourceName,
      VeniceSystemFactory factory,
      SystemProducer producer) {
    this.topicName = resourceName;
    executor = Executors.newSingleThreadExecutor(new DaemonThreadFactory("RouterBasedPushMonitor"));
    pushMonitorTask = new PushMonitorTask(d2TransportClient, topicName, this, factory, producer);
  }

  public void start() {
    executor.submit(pushMonitorTask);
  }

  @Override
  public void close() {
    pushMonitorTask.close();
  }

  public void setCurrentStatus(ExecutionStatus currentStatus) {
    this.currentStatus = currentStatus;
  }

  public ExecutionStatus getCurrentStatus() {
    return this.currentStatus;
  }

  public void setStreamReprocessingExitMode(SamzaExitMode exitMode) {
    this.pushMonitorTask.exitMode = exitMode;
  }

  private static class PushMonitorTask implements Runnable, Closeable {
    private static final ObjectMapper MAPPER = ObjectMapperFactory.getInstance();

    private final AtomicBoolean isRunning;
    private final String topicName;
    private final TransportClient transportClient;
    private final String requestPath;
    private final RouterBasedPushMonitor pushMonitorService;
    private final VeniceSystemFactory factory;
    private final SystemProducer producer;

    private SamzaExitMode exitMode = SamzaExitMode.SUCCESS_EXIT;

    public PushMonitorTask(
        TransportClient transportClient,
        String topicName,
        RouterBasedPushMonitor pushMonitorService,
        VeniceSystemFactory factory,
        SystemProducer producer) {
      this.transportClient = transportClient;
      this.topicName = topicName;
      this.requestPath = buildPushStatusRequestPath(topicName);
      this.pushMonitorService = pushMonitorService;
      this.factory = factory;
      this.producer = producer;
      this.isRunning = new AtomicBoolean(true);
    }

    @Override
    public void run() {
      LOGGER.info("Running {}", this.getClass().getSimpleName());
      while (isRunning.get()) {
        try {
          // Get push status
          CompletableFuture<TransportClientResponse> responseFuture = transportClient.get(requestPath);
          TransportClientResponse response = responseFuture.get(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
          PushStatusResponse pushStatusResponse = MAPPER.readValue(response.getBody(), PushStatusResponse.class);
          if (pushStatusResponse.isError()) {
            LOGGER.error("Router was not able to get push status: {}", pushStatusResponse.getError());
            continue;
          }
          pushMonitorService.setCurrentStatus(pushStatusResponse.getExecutionStatus());
          switch (pushStatusResponse.getExecutionStatus()) {
            case END_OF_PUSH_RECEIVED:
            case COMPLETED:
              LOGGER.info("Samza stream reprocessing has finished successfully for store version: {}", topicName);
              factory.endStreamReprocessingSystemProducer(producer, true);
              /**
               * If there is no more active samza producer, check whether all the stream reprocessing jobs succeed;
               * if so, exit the Samza task; otherwise, let user knows some jobs have failed.
               *
               * If there is any real-time Samza producer created in the factory, the number of active producers would
               * never be 0, so a COMPLETED stream reprocessing job will not accidentally stop the real-time job.
               *
               * TODO: there is a potential edge case that needs to be fixed:
               * 1. User's application start stream reprocessing jobs a number of stores at the beginning;
               * 2. User send EOP to all the stores in #1 through admin-tool or Nuage to end all the jobs;
               * 3. All the jobs complete successfully, so the active number of producers drop to 0 and the Samza job exits;
               * 4. However.. user's application has some special logic to sleep a few hours and start new stream reprocessing
               *    jobs for stores that are not included in #1; in this case, Samza already exit and these new jobs will
               *    never start.
               * Unfortunately there is no solution to the above edge case at the moment; we can only pause the exit process
               * a bit and then check whether the active number of producers is still 0.
               */
              if (factory.getNumberOfActiveSystemProducers() == 0) {
                // Pause a bit just in case user suddenly create new producers from the factory
                LOGGER.info("Pause 30 seconds before exiting the Samza process.");
                Utils.sleep(30000);
                if (factory.getNumberOfActiveSystemProducers() != 0) {
                  break;
                }
                if (factory.getOverallExecutionStatus()) {
                  /**
                   * All stream reprocessing jobs succeed; exit the task based on the selected exit mode
                   */
                  LOGGER.info("Exiting Samza process after all stream reprocessing jobs succeeded.");
                  exitMode.exit();
                } else {
                  throw new VeniceException("Not all stream reprocessing jobs succeeded.");
                }
              }
              return;
            case ERROR:
              LOGGER.info("Stream reprocessing job failed for store version: {}", topicName);
              factory.endStreamReprocessingSystemProducer(producer, false);
              // Stop polling
              return;
            default:
              LOGGER.info(
                  "Current stream reprocessing job state: {} for store version: {}",
                  pushStatusResponse.getExecutionStatus(),
                  topicName);
          }

          Utils.sleep(POLL_CYCLE_DELAY_MS);
        } catch (Exception e) {
          LOGGER.error("Error when polling push status from router for store version: {}", topicName, e);
        }
      }
    }

    @Override
    public void close() {
      isRunning.getAndSet(false);
    }

    private static String buildPushStatusRequestPath(String topicName) {
      return TYPE_PUSH_STATUS + "/" + topicName;
    }
  }
}
