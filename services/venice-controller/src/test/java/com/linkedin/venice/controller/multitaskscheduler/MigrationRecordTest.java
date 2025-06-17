package com.linkedin.venice.controller.multitaskscheduler;

import static com.linkedin.venice.controller.multitaskscheduler.MigrationRecord.Step;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.time.Instant;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class MigrationRecordTest {
  @DataProvider(name = "stepMappings")
  public Object[][] stepMappings() {
    return new Object[][] { { 0, Step.CHECK_DISK_SPACE }, { 1, Step.PRE_CHECK_AND_SUBMIT_MIGRATION_REQUEST },
        { 2, Step.VERIFY_MIGRATION_STATUS }, { 3, Step.UPDATE_CLUSTER_DISCOVERY }, { 4, Step.VERIFY_READ_REDIRECTION },
        { 5, Step.END_MIGRATION }, { 6, Step.MIGRATION_SUCCEED } };
  }

  @Test(dataProvider = "stepMappings")
  public void fromStepNumberReturnsCorrectEnum(int step, Step expected) {
    assertEquals(MigrationRecord.Step.fromStepNumber(step), expected);
    assertEquals(MigrationRecord.Step.fromStepNumber(step).getStepNumber(), step);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void fromStepNumberThrowsOnInvalidValue() {
    MigrationRecord.Step.fromStepNumber(99);
  }

  @Test
  public void builderPopulatesAllFields() {
    MigrationRecord record = new MigrationRecord.Builder("store-test", "cluster-src", "cluster-dst").currentStep(2)
        .attempts(3)
        .aborted(true)
        .abortOnFailure(false)
        .build();

    assertEquals(record.getStoreName(), "store-test");
    assertEquals(record.getSourceCluster(), "cluster-src");
    assertEquals(record.getDestinationCluster(), "cluster-dst");
    assertEquals(record.getCurrentStepEnum(), MigrationRecord.Step.VERIFY_MIGRATION_STATUS);
    assertEquals(record.getAttempts(), 3);
    assertTrue(record.getIsAborted());
    assertFalse(record.getAbortOnFailure());
    // builder leaves start-time at default (epochMilli = ‑1)
    assertEquals(record.getStoreMigrationStartTime(), Instant.ofEpochMilli(-1));
  }

  @Test
  public void incrementAndResetAttemptsWork() {
    MigrationRecord rec = new MigrationRecord.Builder("test-store", "sc", "dc").build();
    assertEquals(rec.getAttempts(), 0);

    rec.incrementAttempts();
    rec.incrementAttempts();
    assertEquals(rec.getAttempts(), 2);

    rec.resetAttempts();
    assertEquals(rec.getAttempts(), 0);
  }

  @Test
  public void setCurrentStepByEnumAndNumberWork() {
    MigrationRecord rec = new MigrationRecord.Builder("test-store", "sc", "dc").build();

    rec.setCurrentStep(MigrationRecord.Step.END_MIGRATION);
    assertEquals(rec.getCurrentStepEnum(), MigrationRecord.Step.END_MIGRATION);

    rec.setCurrentStep(6); // MIGRATION_SUCCEED
    assertEquals(rec.getCurrentStepEnum(), MigrationRecord.Step.MIGRATION_SUCCEED);

    assertTrue(rec.getAbortOnFailure());
    assertFalse(rec.getIsAborted());
  }

  @Test
  @SuppressWarnings("deprecation")
  public void deprecatedConstructorKeepsLegacyDefaults() {
    MigrationRecord rec = new MigrationRecord("store-test", "srcC", "dstC", 0);

    assertEquals(rec.getCurrentStepEnum(), MigrationRecord.Step.CHECK_DISK_SPACE);
    assertEquals(rec.getAttempts(), 0);
    assertEquals(rec.getStoreMigrationStartTime(), Instant.ofEpochMilli(-1));
    assertTrue(rec.getAbortOnFailure());
    assertFalse(rec.getIsAborted());
  }

}
