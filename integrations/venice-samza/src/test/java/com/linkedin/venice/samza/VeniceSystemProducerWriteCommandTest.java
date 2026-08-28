package com.linkedin.venice.samza;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


/**
 * Deterministic tests for the two-future ({@code submission} + {@code durable}) state machine of
 * {@link VeniceSystemProducerWriteCommand} and the {@link VeniceSystemProducerWriteCommand#awaitSubmission}
 * contract that public {@code put}/{@code delete} and the Samza envelope {@code send} rely on. No sleeps are
 * used for positive waits; a small bounded window is only used to assert something does NOT happen.
 */
public class VeniceSystemProducerWriteCommandTest {
  private static final int AWAIT_SECONDS = 10;
  private static final long NEGATIVE_CHECK_MS = 300;

  @Test
  public void submitInvokesMatchingWriterOperationExactlyOnce() {
    @SuppressWarnings("unchecked")
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mock(AbstractVeniceWriter.class);
    PubSubProducerCallback callback = mock(PubSubProducerCallback.class);

    byte[] key = new byte[] { 1, 2 };
    byte[] value = new byte[] { 3, 4 };
    VeniceSystemProducerWriteCommand.put(key, value, 7, 100L).submit(writer, callback);
    verify(writer).put(key, value, 7, 100L, callback);

    VeniceSystemProducerWriteCommand.update(key, value, 7, 9, 100L).submit(writer, callback);
    verify(writer).update(key, value, 7, 9, 100L, callback);

    VeniceSystemProducerWriteCommand.delete(key, 100L).submit(writer, callback);
    verify(writer).delete(key, 100L, callback);
  }

  @Test
  public void serializedPayloadIsSnapshotBeforeEnqueue() {
    // The caller serializes before building the command; the command carries that exact snapshot to the
    // worker, so the writer must receive the very bytes captured at construction time.
    @SuppressWarnings("unchecked")
    AbstractVeniceWriter<byte[], byte[], byte[]> writer = mock(AbstractVeniceWriter.class);
    byte[] key = new byte[] { 10, 20 };
    byte[] value = new byte[] { 30, 40 };
    VeniceSystemProducerWriteCommand command = VeniceSystemProducerWriteCommand.put(key, value, 3, 5L);
    assertEquals(command.getKey(), key);
    command.submit(writer, mock(PubSubProducerCallback.class));
    verify(writer).put(eq(key), eq(value), eq(3), eq(5L), any());
  }

  @Test
  public void syncSuccessThenAsyncSuccessCompletesBothInOrder() throws Exception {
    VeniceSystemProducerWriteCommand command =
        VeniceSystemProducerWriteCommand.put(new byte[] { 1 }, new byte[] { 2 }, 1, 0L);
    CompletableFuture<Void> durable = command.getDurableFuture();
    CompletableFuture<Void> submission = command.getDurableFuture().getSubmissionFuture();

    command.finishSubmission(null);
    submission.get(AWAIT_SECONDS, TimeUnit.SECONDS);
    assertFalse(durable.isDone(), "durable must wait for the async callback");

    Runnable durableCompletion = command.registerCallback(null);
    assertNotNull(durableCompletion, "an async callback after submission owes the durable completion");
    durableCompletion.run();
    durable.get(AWAIT_SECONDS, TimeUnit.SECONDS);
  }

  @Test
  public void asyncCallbackBeforeSubmissionIsDeferredThenCompletes() throws Exception {
    VeniceSystemProducerWriteCommand command =
        VeniceSystemProducerWriteCommand.put(new byte[] { 1 }, new byte[] { 2 }, 1, 0L);
    CompletableFuture<Void> durable = command.getDurableFuture();

    // Callback can race ahead of submission; it must be deferred, not lost.
    Runnable early = command.registerCallback(null);
    assertNull(early, "a callback before submission is deferred, not completed");
    assertFalse(durable.isDone(), "durable must not complete before submission is done");

    Runnable durableCompletion = command.finishSubmission(null);
    assertNotNull(durableCompletion, "submission after a deferred callback owes the durable completion");
    durableCompletion.run();
    durable.get(AWAIT_SECONDS, TimeUnit.SECONDS);
  }

  @Test
  public void syncFailureFailsBothFuturesAndBlocksLateCallback() throws Exception {
    VeniceSystemProducerWriteCommand command =
        VeniceSystemProducerWriteCommand.put(new byte[] { 1 }, new byte[] { 2 }, 1, 0L);
    CompletableFuture<Void> durable = command.getDurableFuture();
    CompletableFuture<Void> submission = command.getDurableFuture().getSubmissionFuture();
    RuntimeException failure = new VeniceException("sync writer failure");

    Runnable durableFailure = command.finishSubmission(failure);
    assertNotNull(durableFailure, "a sync failure owes the durable completion");
    assertSame(expectCause(submission), failure);
    durableFailure.run();

    // A late async callback must not override the sticky synchronous failure.
    Runnable late = command.registerCallback(null);
    assertNull(late, "a late callback after a sync failure is ignored");
    assertSame(expectCause(durable), failure);
  }

  @Test
  public void asyncFailureFailsDurableButNotSubmission() throws Exception {
    VeniceSystemProducerWriteCommand command =
        VeniceSystemProducerWriteCommand.put(new byte[] { 1 }, new byte[] { 2 }, 1, 0L);
    CompletableFuture<Void> durable = command.getDurableFuture();
    CompletableFuture<Void> submission = command.getDurableFuture().getSubmissionFuture();
    RuntimeException brokerFailure = new VeniceException("broker failure");

    command.finishSubmission(null);
    submission.get(AWAIT_SECONDS, TimeUnit.SECONDS);

    Runnable durableFailure = command.registerCallback(brokerFailure);
    assertNotNull(durableFailure, "an async failure after submission owes the durable completion");
    durableFailure.run();
    assertSame(expectCause(durable), brokerFailure);
  }

  @Test
  public void awaitSubmissionIsNoOpForForeignFuture() throws Exception {
    // An inline path or a subclass override returns a plain future; awaitSubmission must never wait on or
    // cast it, so an intentionally-never-completed foreign future returns immediately.
    CompletableFuture<Void> foreign = new CompletableFuture<>();
    CountDownLatch returned = new CountDownLatch(1);
    Thread t = new Thread(() -> {
      VeniceSystemProducerWriteCommand.awaitSubmission(foreign);
      returned.countDown();
    });
    t.start();
    assertTrue(returned.await(AWAIT_SECONDS, TimeUnit.SECONDS), "awaitSubmission must not wait on a foreign future");
    t.join();
    assertFalse(foreign.isDone());
  }

  @Test
  public void awaitSubmissionWaitsForSubmissionOfDurableFuture() throws Exception {
    VeniceSystemProducerWriteCommand command =
        VeniceSystemProducerWriteCommand.put(new byte[] { 1 }, new byte[] { 2 }, 1, 0L);
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch returned = new CountDownLatch(1);
    Thread t = new Thread(() -> {
      started.countDown();
      VeniceSystemProducerWriteCommand.awaitSubmission(command.getDurableFuture());
      returned.countDown();
    });
    t.start();
    assertTrue(started.await(AWAIT_SECONDS, TimeUnit.SECONDS));
    assertFalse(returned.await(NEGATIVE_CHECK_MS, TimeUnit.MILLISECONDS), "must block until submission completes");

    command.finishSubmission(null);
    assertTrue(returned.await(AWAIT_SECONDS, TimeUnit.SECONDS), "must return once submission completes");
    t.join();
  }

  @Test
  public void awaitSubmissionSurfacesSubmissionFailure() {
    VeniceSystemProducerWriteCommand command =
        VeniceSystemProducerWriteCommand.put(new byte[] { 1 }, new byte[] { 2 }, 1, 0L);
    RuntimeException failure = new VeniceException("submission failed");
    command.finishSubmission(failure);
    try {
      VeniceSystemProducerWriteCommand.awaitSubmission(command.getDurableFuture());
      fail("awaitSubmission should surface the submission failure");
    } catch (RuntimeException e) {
      assertSame(e, failure);
    }
  }

  @Test
  public void awaitSubmissionRethrowsErrorCauseUnchanged() throws Exception {
    // A fatal Error completed into the state machine must reach both futures as the cause AND be rethrown by
    // awaitSubmission with its original identity (not wrapped in a VeniceException).
    VeniceSystemProducerWriteCommand command =
        VeniceSystemProducerWriteCommand.put(new byte[] { 1 }, new byte[] { 2 }, 1, 0L);
    FatalTestError fatal = new FatalTestError();
    Runnable durableFailure = command.finishSubmission(fatal);
    assertNotNull(durableFailure, "a fatal Error owes the durable completion");
    durableFailure.run();

    assertSame(expectCause(command.getDurableFuture().getSubmissionFuture()), fatal);
    assertSame(expectCause(command.getDurableFuture()), fatal);
    try {
      VeniceSystemProducerWriteCommand.awaitSubmission(command.getDurableFuture());
      fail("awaitSubmission should rethrow the fatal Error");
    } catch (Error e) {
      assertSame(e, fatal);
    }
  }

  private static Throwable expectCause(CompletableFuture<Void> future) throws Exception {
    try {
      future.get(AWAIT_SECONDS, TimeUnit.SECONDS);
      fail("future should have completed exceptionally");
      return null;
    } catch (ExecutionException e) {
      return e.getCause();
    }
  }

  /** A distinct Error subtype so the identity-preservation assertions cannot accidentally match. */
  private static final class FatalTestError extends Error {
    private static final long serialVersionUID = 1L;
  }
}
