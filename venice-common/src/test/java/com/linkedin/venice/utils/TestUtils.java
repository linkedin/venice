package com.linkedin.venice.utils;

import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import javax.validation.constraints.NotNull;


/**
 * General-purpose utility functions for tests.
 */
public class TestUtils {
  /** In milliseconds */
  private static final int WAIT_TIME_FOR_NON_DETERMINISTIC_ACTIONS = 30;

  public static String getUniqueString(String base) {
    return base + "-" + System.currentTimeMillis() + "-" + RandomGenUtils.getRandomIntWithIn(Integer.MAX_VALUE);
  }

  /**
   * To be used for tests when we need to wait for an async operation to complete.  Pass a timeout, and a labmda
   * for checking if the operation is complete.
   *
   * @param timeout amount of time to wait
   * @param timeoutUnits {@link TimeUnit} for the {@param timeout}
   * @param conditionToWaitFor A {@link BooleanSupplier} which should execute the non-deterministic action and
   *                           return true if it is successful, false otherwise.
   */
  public static void waitForNonDeterministicCompletion(long timeout, TimeUnit timeoutUnits, BooleanSupplier conditionToWaitFor){
    long timeoutTime = System.currentTimeMillis() + timeoutUnits.toMillis(timeout);
    while (!conditionToWaitFor.getAsBoolean()){
      if (System.currentTimeMillis() > timeoutTime){
        throw new RuntimeException("Operation did not complete in time");
      }
      Utils.sleep(WAIT_TIME_FOR_NON_DETERMINISTIC_ACTIONS);
    }
  }

  /**
   * To be used for tests when we need to wait for an async operation to complete. Pass a timeout, and a labmda
   * for checking if the operation is complete.
   *
   * @param timeout amount of time to wait
   * @param timeoutUnits {@link TimeUnit} for the {@param timeout}
   * @param assertionToWaitFor A {@link NonDeterministicAssertion} which should simply execute without exception
   *                           if it is successful, or throw an {@link AssertionError} otherwise.
   * @throws AssertionError throws the exception thrown by the {@link NonDeterministicAssertion} if the maximum
   *                        wait time has been exceeded.
   */
  public static void waitForNonDeterministicAssertion(long timeout,
                                                      TimeUnit timeoutUnits,
                                                      NonDeterministicAssertion assertionToWaitFor) throws AssertionError {
    long timeoutTime = System.currentTimeMillis() + timeoutUnits.toMillis(timeout);
    while (true) {
      try {
        assertionToWaitFor.execute();
        return;
      } catch (AssertionError ae) {
        if (System.currentTimeMillis() > timeoutTime) {
          throw ae;
        }
        Utils.sleep(WAIT_TIME_FOR_NON_DETERMINISTIC_ACTIONS);
      }
    }
  }

  public static Store createTestStore(@NotNull String name, @NotNull String owner, long createdTime) {
      return new Store(name, owner, createdTime, PersistenceType.IN_MEMORY, RoutingStrategy.CONSISTENT_HASH,
          ReadStrategy.ANY_OF_ONLINE, OfflinePushStrategy.WAIT_ALL_REPLICAS);
  }

  public interface NonDeterministicAssertion {
    void execute() throws AssertionError;
  }
}
