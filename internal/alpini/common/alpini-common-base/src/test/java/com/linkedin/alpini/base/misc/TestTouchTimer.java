/*
 * $Id$
 */
package com.linkedin.alpini.base.misc;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * @author Jemiah Westerman &lt;jwesterman@linkedin.com&gt;
 *
 * @version $Revision$
 */
public class TestTouchTimer {
  @Test(groups = "unit")
  public void test1() throws Exception {
    synchronized (Time.class) {
      try {
        Time.freeze();
        test0(new TouchTimer(), Pattern.compile("Total Time: 0ms Trace: "));
      } finally {
        Time.restore();
      }
    }
  }

  @Test(groups = "unit")
  public void test2() throws Exception {
    synchronized (Time.class) {
      try {
        Time.freeze();
        long millis = Time.currentTimeMillis();
        long nanos = Time.nanoTime();
        Thread.sleep(2);
        test0(new TouchTimer(millis, nanos, 5), Pattern.compile("Total Time: \\dms Trace: \\[[^]]+ start [^]]+]"));
      } finally {
        Time.restore();
      }
    }
  }

  private void test0(final TouchTimer timer, Pattern emptyRegexp) throws Exception {

    // Test functions on an empty timer
    Assert.assertEquals(timer.getElapsedTimeMillis(), 0);
    Assert.assertTrue(emptyRegexp.matcher(timer.toString()).matches(), timer.toString());

    // Add a few touches and test functions again
    timer.touch(getClass(), "first");
    Time.advance(2, TimeUnit.MILLISECONDS);
    timer.touch(getClass(), "second(%s)", "string");
    Time.advance(2, TimeUnit.MILLISECONDS);
    timer.touch(getClass(), "third");

    Assert.assertTrue(
        timer.getElapsedTimeMillis() >= 4,
        "Expected total delta >= 4. Was: " + timer.getElapsedTimeMillis());

    Time.restore();

    // Touch from another thread to make sure we get the thread name in the final output
    Thread t = new Thread("Test Thread") {
      @Override
      public void run() {
        timer.touch("from another thread");
      }
    };
    t.start();
    t.join();

    // Make sure the final output has all the important bits of info
    String s = timer.toString();
    Assert.assertTrue(s.contains("first"), "toString was missing \"first\" touch.");
    Assert.assertTrue(s.contains("second(string)"), "toString was missing \"second\" touch.");
    Assert.assertTrue(s.contains("third"), "toString was missing \"third\" touch.");
    Assert.assertTrue(s.contains("from another thread"), "toString was missing \"from another thread\" touch.");
    Assert.assertTrue(s.contains("Test Thread"), "toString was missing thread name \"Test Thread\".");
    Assert.assertTrue(s.contains("TestTouchTimer"), "toString was missing the class name \"TestTouchTimer\".");
  }

  @Test(groups = "unit")
  public void testStringDedup() {
    // Make sure thread names are deduped
    Assert.assertSame(
        TouchTimer.getDedupedThreadName(),
        TouchTimer.getDedupedThreadName(),
        "Thread name was not de-duped.");

    // Create a timer and add a couple touches. Then check that they are properly deduped.
    final TouchTimer timer = new TouchTimer();
    String s = "This is a String";
    timer.touch(s);
    timer.touch(s);

    // Make sure that the actual message is not duplicated. Since we touched with the same String instance twice, this
    // should
    // be the case.
    List<TouchTimer.Message> messages = timer.getMessages();
    Assert.assertEquals(messages.size(), 2);
    Assert.assertSame(messages.get(0)._name, s);
    Assert.assertSame(messages.get(1)._name, s);

    // Make sure the thread name is not duplicated
    Assert.assertSame(
        messages.get(0)._threadName,
        messages.get(1)._threadName,
        "_threadName for two messages should reference the same String instance. It is not sufficiet that they have the same name.");
  }

  @Test(groups = "unit")
  public void testMessageCutoff() {
    final TouchTimer timer = new TouchTimer();
    for (int i = 1; i < TouchTimer.DEFAULT_MAX_MESSAGES + 50; i++)
      timer.touch(String.valueOf(i));

    // We expect DEFAULT_MAX_MESSAGES from the application, plus one additional message at the end saying we're not
    // adding any more messages
    // Total expected is DEFAULT_MAX_MESSAGES + 1
    List<TouchTimer.Message> messages = timer.getMessages();
    Assert.assertEquals(messages.size(), TouchTimer.DEFAULT_MAX_MESSAGES + 1);
    // Make sure the last message is the error message
    Assert.assertTrue(
        messages.get(messages.size() - 1)._name
            .contains("TouchTimer Warning: Exceeded the maximum number of messages allowed"));
    // Make sure the last message is the one we expect.
    Assert.assertEquals(messages.get(messages.size() - 2)._name, String.valueOf(TouchTimer.DEFAULT_MAX_MESSAGES));

    TouchTimer.Visitor visitor = Mockito.mock(TouchTimer.Visitor.class);
    timer.forEachMessage(visitor);

    Mockito.verify(visitor, Mockito.times(TouchTimer.DEFAULT_MAX_MESSAGES + 1))
        .visit(Mockito.anyLong(), Mockito.anyString(), Mockito.any(), Mockito.anyString(), Mockito.any());
    Mockito.verifyNoMoreInteractions(visitor);
  }

  @Test(groups = "unit")
  public void testEmptyTimer() {
    final TouchTimer timer = new TouchTimer();
    // Make sure these methods behave as expected on a timer with no messages.
    Assert.assertEquals(timer.getMessages().size(), 0);
    Assert.assertEquals(timer.getElapsedTimeMillis(), 0);

    TouchTimer.Visitor visitor = Mockito.mock(TouchTimer.Visitor.class);
    timer.forEachMessage(visitor);
    Mockito.verifyNoMoreInteractions(visitor);
  }

}
