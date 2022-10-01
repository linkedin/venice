package com.linkedin.venice.hadoop.pbnj;

import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.hadoop.utils.HadoopUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class only supports Avro input because Avro and Vson have different input format.
 * You'd better refactor it before using it for Vson input :D
 */

public class PostBulkLoadAnalysisMapper
    implements Mapper<AvroWrapper<IndexedRecord>, NullWritable, NullWritable, NullWritable> {
  private static final Logger LOGGER = LogManager.getLogger(PostBulkLoadAnalysisMapper.class);

  private static final int NUM_THREADS = 200;
  private static final int REQUEST_TIME_OUT_MS = 10000;
  private static final int THROTTLE_SLEEP_TIME_ON_QUOTA_EXCEPTION_MS = 200;
  private static final int CLOSE_LOOP_TIME_OUT_MS = 1000;
  private static final int SAMPLE_LOGGING_INTERVAL = 1000;

  // Facilities
  private Progressable progress;
  private ExecutorService executor;
  private CompletionService<Data> completionService;
  private AvroGenericStoreClient<Object, Object> veniceClient;

  /** Immutable state (not final because it is set by {@link #configure(JobConf)} */
  private String keyField;
  private String valueField;
  private int keyFieldPos;
  private int valueFieldPos;
  private boolean failFast;
  private boolean async;
  private double samplingRatio;
  private Sampler sampler;

  // Mutable state
  private final AtomicBoolean closeCalled = new AtomicBoolean(false);
  private final AtomicReference<Exception> sendException = new AtomicReference<>();
  private final AtomicLong goodRecords = new AtomicLong();
  private final AtomicLong queriedRecords = new AtomicLong();
  private final AtomicLong skippedRecords = new AtomicLong(); // Skipped because of sampling
  private final AtomicLong badRecords = new AtomicLong();

  @Override
  public void configure(JobConf job) {
    VeniceProperties props = HadoopUtils.getVeniceProps(job);
    LOGGER.info(this.getClass().getSimpleName() + " to be constructed with props: " + props.toString(true));

    // Set up config
    this.failFast = props.getBoolean(VenicePushJob.PBNJ_FAIL_FAST);
    this.async = props.getBoolean(VenicePushJob.PBNJ_ASYNC);
    this.samplingRatio = props.getDouble(VenicePushJob.PBNJ_SAMPLING_RATIO_PROP);
    this.keyField = props.getString(VenicePushJob.KEY_FIELD_PROP);
    this.valueField = props.getString(VenicePushJob.VALUE_FIELD_PROP);
    String schemaStr = props.getString(VenicePushJob.SCHEMA_STRING_PROP);
    this.keyFieldPos = getFieldPos(schemaStr, keyField);
    this.valueFieldPos = getFieldPos(schemaStr, valueField);

    if (async) {
      // There is a bug with the async mode, because we are hanging on to references which
      // get mutated. We would need to do a deep copy of the data in order to support this
      // mode. I am going to hold on for now because deep copy will be easier to implement
      // after we upgrade to Avro 1.7.
      // TODO: Remove this check once the async mode is fixed.
      throw new VeniceException("Async mode not currently supported in PBNJ.");
    }

    this.veniceClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(props.getString(VenicePushJob.VENICE_STORE_NAME_PROP))
            .setVeniceURL(props.getString(VenicePushJob.PBNJ_ROUTER_URL_PROP)));

    this.sampler = new Sampler(samplingRatio);

    LOGGER.info("veniceClient started: " + veniceClient.toString());

    if (async) {
      this.executor = Executors.newFixedThreadPool(NUM_THREADS);
      this.executor.execute(new CompletionTask(this));
      this.completionService = new ExecutorCompletionService<>(executor);
      LOGGER.info("Async mode activated. CompletionTask started.");
    } else {
      this.executor = null;
      this.completionService = null;
    }

    LOGGER.info(this.getClass().getSimpleName() + " constructed.");
  }

  private boolean isRunning() {
    if (sendException.get() != null && failFast) {
      return false;
    } else if (closeCalled.get()) {
      return queriedRecords.get() < goodRecords.get() + badRecords.get();
    } else {
      return true;
    }
  }

  private int getFieldPos(String schemaStr, String field) {
    return Schema.parse(schemaStr).getField(field).pos();
  }

  @Override
  public void close() throws IOException {
    LOGGER.info("Records progress before closing client:");
    logMessageProgress();

    closeCalled.set(true);

    while (isRunning()) {
      try {
        Thread.sleep(CLOSE_LOOP_TIME_OUT_MS);
      } catch (InterruptedException e) {
        throw new VeniceException("Interrupted!", e);
      }
    }

    if (executor != null) {
      executor.shutdown();
      try {
        executor.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    veniceClient.close();

    maybePropagateCallbackException();
    LOGGER.info("Records progress after flushing and closing producer:");
    logMessageProgress();
    if (goodRecords.get() != queriedRecords.get()) {
      throw new VeniceException("Good records: " + goodRecords + " don't match total records: " + queriedRecords);
    }
  }

  @Override
  public void map(
      AvroWrapper<IndexedRecord> record,
      NullWritable value,
      OutputCollector<NullWritable, NullWritable> output,
      Reporter reporter) throws IOException {
    if (failFast) {
      maybePropagateCallbackException();
    }

    if (sampler.checkWhetherToSkip(queriedRecords.get(), skippedRecords.get())) {
      skippedRecords.incrementAndGet();
      return;
    }

    // Initialize once
    if (this.progress == null) {
      this.progress = reporter;
    }
    IndexedRecord datum = record.datum();
    Object keyDatum = datum.get(keyFieldPos);
    if (keyDatum == null) {
      // Invalid data
      // Theoretically it should not happen since all the avro records are sharing the same schema in the same file
      throw new VeniceException("Encountered record with null key");
    }

    Object valueDatumFromHdfs = datum.get(valueFieldPos);

    if (queriedRecords.get() % SAMPLE_LOGGING_INTERVAL == 0) {
      logMessageProgress();
    }

    Callable<Data> callable = () -> {
      long timeSlept = 0;
      while (true) {
        try {
          return new Data(
              keyDatum,
              valueDatumFromHdfs,
              veniceClient.get(keyDatum).get(REQUEST_TIME_OUT_MS, TimeUnit.MILLISECONDS));
        } catch (VeniceClientHttpException e) {
          if (timeSlept > REQUEST_TIME_OUT_MS) {
            throw new VeniceException(
                "Could not successfully complete query because of VeniceClientHttpException even after sleeping a total of "
                    + REQUEST_TIME_OUT_MS + " in-between requests.",
                e);
          }
          Thread.sleep(THROTTLE_SLEEP_TIME_ON_QUOTA_EXCEPTION_MS);
          timeSlept += THROTTLE_SLEEP_TIME_ON_QUOTA_EXCEPTION_MS;
        }
      }
    };

    if (async) {
      completionService.submit(callable);
    } else {
      try {
        Data data = callable.call();
        verify(data, this);
      } catch (Exception e) {
        sendException.set(new VeniceException("Exception!", e));
      }
    }
    queriedRecords.incrementAndGet();
  }

  private void maybePropagateCallbackException() {
    if (sendException.get() != null) {
      throw new VeniceException("Post-Bulkload Analysis Job failed with exception", sendException.get());
    }
  }

  private void logMessageProgress() {
    LOGGER.info(
        "Good records: " + goodRecords.get() + ",\t Bad records: " + badRecords.get() + ",\t Queried records: "
            + queriedRecords.get() + ",\t Skipped records: " + skippedRecords.get());
  }

  private static class CompletionTask implements Runnable {
    private final PostBulkLoadAnalysisMapper pbnj;

    CompletionTask(PostBulkLoadAnalysisMapper pbnj) {
      this.pbnj = pbnj;
    }

    @Override
    public void run() {
      LOGGER.info("Start of " + CompletionTask.class.getSimpleName());
      while (pbnj.isRunning()) {
        try {
          Data data = pbnj.completionService.take().get(REQUEST_TIME_OUT_MS, TimeUnit.MILLISECONDS);
          verify(data, pbnj);
        } catch (InterruptedException e) {
          pbnj.sendException.set(new VeniceException("Interrupted!", e));
        } catch (TimeoutException e) {
          pbnj.sendException.set(new VeniceException("Timeout!", e));
        } catch (ExecutionException e) {
          // TODO: We might need to back off and try again here, instead of failing fast.
          pbnj.sendException.set(new VeniceException("ExecutionException!", e));
        } finally {
          pbnj.progress.progress();
        }
      }
    }
  }

  private static class Data {
    private final Object keyDatum, valueDatumFromHdfs, valueFromVenice;

    Data(Object keyDatum, Object valueDatumFromHdfs, Object valueFromVenice) {
      this.keyDatum = keyDatum;
      this.valueDatumFromHdfs = valueDatumFromHdfs;
      this.valueFromVenice = valueFromVenice;
    }
  }

  private static void verify(Data data, PostBulkLoadAnalysisMapper pbnj) {
    if ((data.valueDatumFromHdfs == null && data.valueFromVenice == null)
        || (data.valueDatumFromHdfs != null && data.valueDatumFromHdfs.equals(data.valueFromVenice))) {
      // Both null, or both equal
      pbnj.goodRecords.incrementAndGet();
    } else {
      pbnj.badRecords.incrementAndGet();
      String exceptionMessage = "Records don't match for key: " + data.keyDatum.toString();
      LOGGER.error(exceptionMessage);
      LOGGER.error("Value read from HDFS: {}", data.valueDatumFromHdfs);
      LOGGER.error("Value read from Venice: {}", data.valueFromVenice);
      pbnj.sendException.set(new VeniceException(exceptionMessage));
    }
  }
}
