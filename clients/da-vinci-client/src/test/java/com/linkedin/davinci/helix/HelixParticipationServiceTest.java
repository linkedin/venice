package com.linkedin.davinci.helix;

import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.expectThrows;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixPartitionStatusAccessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.Test;


public class HelixParticipationServiceTest {
  private static final Logger LOGGER = LogManager.getLogger(HelixParticipationServiceTest.class);

  @Test
  public void testResetAllInstanceCVStates() {
    HelixPartitionStatusAccessor mockAccessor = mock(HelixPartitionStatusAccessor.class);
    StorageService mockStorageService = mock(StorageService.class);

    // Call the reset method
    HelixParticipationService.resetAllInstanceCVStates(mockAccessor, mockStorageService, LOGGER);

    // Verify that the bulk delete API is called instead of per-partition deletion
    verify(mockAccessor).deleteAllCustomizedStates();
  }

  @Test
  public void testResetAllInstanceCVStatesWithException() {
    HelixPartitionStatusAccessor mockAccessor = mock(HelixPartitionStatusAccessor.class);
    StorageService mockStorageService = mock(StorageService.class);

    // Make the delete operation throw an exception
    VeniceException testException = new VeniceException("Test exception during CV cleanup");
    doThrow(testException).when(mockAccessor).deleteAllCustomizedStates();

    // Call the reset method and expect it to throw the exception
    expectThrows(
        VeniceException.class,
        () -> HelixParticipationService.resetAllInstanceCVStates(mockAccessor, mockStorageService, LOGGER));

    // Verify that the bulk delete API was attempted
    verify(mockAccessor).deleteAllCustomizedStates();
  }

  @Test
  public void testUnknownHelixInstanceOperation() {
    HelixParticipationService mockHelixParticipationService = mock(HelixParticipationService.class);
    VeniceServerConfig mockServerConfig = mock(VeniceServerConfig.class);

    when(mockServerConfig.isHelixJoinAsUnknownEnabled()).thenReturn(true);

    doCallRealMethod().when(mockHelixParticipationService).buildHelixManagerProperty(mockServerConfig);
    mockHelixParticipationService.buildHelixManagerProperty(mockServerConfig);
  }
}
