package com.linkedin.venice.router;

import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.router.httpclient.R2StorageNodeClient;
import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.Test;


public class TestR2StorageNodeClient {
  @Test
  public void testRandomClientSelection() {
    R2StorageNodeClient client = mock(R2StorageNodeClient.class);
    List<Client> clientList = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      clientList.add(mock(Client.class));
      try {
        client.getRandomClientFromList(clientList);
      } catch (Exception e) {
        fail("Should not fail fetching random client");
      }
    }
  }
}
