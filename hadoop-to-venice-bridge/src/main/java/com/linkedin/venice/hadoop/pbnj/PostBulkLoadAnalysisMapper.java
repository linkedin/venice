package com.linkedin.venice.hadoop.pbnj;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.AvroStoreClientFactory;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.KafkaPushJob;
import com.linkedin.venice.hadoop.utils.HadoopUtils;
import com.linkedin.venice.utils.VeniceProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link Mapper} implementation which queries Venice using a thin client.
 *
 * N.B.: Even though there are commonalities between this code and the
 * {@link com.linkedin.venice.hadoop.KafkaOutputMapper}, it is intentional to
 * NOT leverage any common code between the two. Since one is a watch dog for the
 * other, it is important to minimize the risk that a change which causes a bug in
 * one does not simultaneously cause a bug in the other.
 */
public class PostBulkLoadAnalysisMapper implements Mapper<AvroWrapper<IndexedRecord>, NullWritable, NullWritable, NullWritable> {
  private static Logger logger = Logger.getLogger(PostBulkLoadAnalysisMapper.class);

  private static final int NUM_THREADS = 200;
  private static final int REQUEST_TIME_OUT_MS = 10000;
  private static final int CLOSE_LOOP_TIME_OUT_MS = 1000;
  private static final int SAMPLE_LOGGING_INTERVAL = 1000;

  // Facilities
  private Progressable progress;
  private Executor executor;
  private CompletionService<Data> completionService;
  private AvroGenericStoreClient<Object, Object> veniceClient;

  // Immutable state
  private String keyField;
  private String valueField;
  private int keyFieldPos;
  private int valueFieldPos;
  private boolean failFast;
  private boolean async;

  // Mutable state
  private final AtomicBoolean closeCalled = new AtomicBoolean(false);
  private final AtomicReference<Exception> sendException = new AtomicReference<>();
  private final AtomicLong goodRecords = new AtomicLong();
  private final AtomicLong totalRecords = new AtomicLong();
  private final AtomicLong badRecords = new AtomicLong();


  @Override
  public void configure(JobConf job) {
    VeniceProperties props = HadoopUtils.getVeniceProps(job);
    logger.info(this.getClass().getSimpleName() + " to be constructed with props: " + props.toString(true));

    // Set up config
    this.failFast = props.getBoolean(KafkaPushJob.PBNJ_FAIL_FAST);
    this.async = props.getBoolean(KafkaPushJob.PBNJ_ASYNC);
    this.keyField = props.getString(KafkaPushJob.AVRO_KEY_FIELD_PROP);
    this.valueField = props.getString(KafkaPushJob.AVRO_VALUE_FIELD_PROP);
    String schemaStr = props.getString(KafkaPushJob.SCHEMA_STRING_PROP);
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

    this.veniceClient = AvroStoreClientFactory.getAndStartAvroGenericStoreClient(
        props.getString(KafkaPushJob.PBNJ_ROUTER_URL_PROP),
        props.getString(KafkaPushJob.VENICE_STORE_NAME_PROP));

    logger.info("veniceClient started: " + veniceClient.toString());

    if (async) {
      this.executor = Executors.newFixedThreadPool(NUM_THREADS);
      this.executor.execute(new CompletionTask(this));
      this.completionService = new ExecutorCompletionService<>(executor);
      logger.info("Async mode activated. CompletionTask started.");
    } else {
      this.executor = null;
      this.completionService = null;
    }

    logger.info(this.getClass().getSimpleName() + " constructed.");
  }

  private boolean isRunning() {
    if (null != sendException.get() && failFast) {
      return false;
    } else if (closeCalled.get()) {
      return totalRecords.get() < goodRecords.get() + badRecords.get();
    } else {
      return true;
    }
  }

  private int getFieldPos(String schemaStr, String field) {
    return Schema.parse(schemaStr).getField(field).pos();
  }

  @Override
  public void close() throws IOException {
    logger.info("Records progress before closing client:");
    logMessageProgress();

    closeCalled.set(true);

    while (isRunning()) {
      try {
        Thread.sleep(CLOSE_LOOP_TIME_OUT_MS);
      } catch (InterruptedException e) {
        throw new VeniceException("Interrupted!", e);
      }
    }

    veniceClient.close();

    maybePropagateCallbackException();
    logger.info("Records progress after flushing and closing producer:");
    logMessageProgress();
    if (goodRecords.get() != totalRecords.get()) {
      throw new VeniceException("Good records: " + goodRecords + " don't match total records: " + totalRecords);
    }
  }

  @Override
  public void map(AvroWrapper<IndexedRecord> record, NullWritable value, OutputCollector<NullWritable, NullWritable> output, Reporter reporter) throws IOException {
    if (failFast) {
      maybePropagateCallbackException();
    }

    // Initialize once
    if (null == this.progress) {
      this.progress = reporter;
    }
    IndexedRecord datum = record.datum();
    Object keyDatum = datum.get(keyFieldPos);
    if (null == keyDatum) {
      // Invalid data
      // Theoretically it should not happen since all the avro records are sharing the same schema in the same file
      throw new VeniceException("Encountered record with null key");
    }

    Object valueDatumFromHdfs = datum.get(valueFieldPos);

    if (totalRecords.get() % SAMPLE_LOGGING_INTERVAL == 0) {
      logMessageProgress();
    }

    Callable<Data> callable = () -> new Data(
        keyDatum,
        valueDatumFromHdfs,
        veniceClient.get(keyDatum).get(REQUEST_TIME_OUT_MS, TimeUnit.MILLISECONDS));

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
    totalRecords.incrementAndGet();
  }


  private void maybePropagateCallbackException() {
    if (null != sendException.get()) {
      throw new VeniceException("Post-Bulkload Analysis Job failed with exception", sendException.get());
    }
  }

  private void logMessageProgress() {
    logger.info("Good records: " + goodRecords.get() +
        ",\t Bad records: " + badRecords.get() +
        ",\t Total records: " + totalRecords.get());
  }

  private static class CompletionTask implements Runnable {
    private final PostBulkLoadAnalysisMapper pbnj;
    CompletionTask(PostBulkLoadAnalysisMapper pbnj) {
      this.pbnj = pbnj;
    }
    @Override
    public void run() {
      logger.info("Start of " + CompletionTask.class.getSimpleName());
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
    if ((null == data.valueDatumFromHdfs && null == data.valueFromVenice) ||
        (null != data.valueDatumFromHdfs && data.valueDatumFromHdfs.equals(data.valueFromVenice))) {
      // Both null, or both equal
      pbnj.goodRecords.incrementAndGet();
    } else {
      pbnj.badRecords.incrementAndGet();
      String exceptionMessage = "Records don't match for key: " + data.keyDatum.toString();
      logger.error(exceptionMessage);
      logger.error("Value read from HDFS: " + data.valueDatumFromHdfs);
      logger.error("Value read from Venice: " + data.valueFromVenice);
      pbnj.sendException.set(new VeniceException(exceptionMessage));
    }
  }
}
