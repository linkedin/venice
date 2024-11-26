package com.linkedin.venice.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.expectThrows;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.participant.protocol.ParticipantMessageKey;
import com.linkedin.venice.participant.protocol.ParticipantMessageValue;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pubsub.manager.TopicManagerRepository;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ParticipantStoreClientsManagerTest {
  private static final String CLUSTER_DISCOVERY_D2_SERVICE_NAME = "d2://venice-cluster-discovery";
  private static final String CLUSTER_NAME = "testCluster";
  private static final String PARTICIPANT_STORE_NAME =
      VeniceSystemStoreUtils.getParticipantStoreNameForCluster(CLUSTER_NAME);
  private D2Client d2Client;
  private TopicManagerRepository topicManagerRepository;
  private VeniceWriterFactory veniceWriterFactory;
  private PubSubTopicRepository pubSubTopicRepository;
  private ParticipantStoreClientsManager participantStoreClientsManager;

  @BeforeMethod
  public void setUp() {
    d2Client = mock(D2Client.class);
    topicManagerRepository = mock(TopicManagerRepository.class);
    veniceWriterFactory = mock(VeniceWriterFactory.class);
    pubSubTopicRepository = new PubSubTopicRepository();
    participantStoreClientsManager = new ParticipantStoreClientsManager(
        d2Client,
        CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        topicManagerRepository,
        veniceWriterFactory,
        pubSubTopicRepository);
  }

  @Test
  public void testGetReader() {
    AvroSpecificStoreClient<ParticipantMessageKey, ParticipantMessageValue> result =
        participantStoreClientsManager.getReader(CLUSTER_NAME);
    assertNotNull(result);
    assertEquals(result.getStoreName(), PARTICIPANT_STORE_NAME);

    // call getReader again with the same cluster name
    AvroSpecificStoreClient<ParticipantMessageKey, ParticipantMessageValue> result2 =
        participantStoreClientsManager.getReader(CLUSTER_NAME);
    assertSame(result, result2);
  }

  @Test
  public void testClose() {
    // get read clients map
    Map<String, AvroSpecificStoreClient<ParticipantMessageKey, ParticipantMessageValue>> readClients =
        participantStoreClientsManager.getReadClients();
    AvroSpecificStoreClient<ParticipantMessageKey, ParticipantMessageValue> rc0 = mock(AvroSpecificStoreClient.class);
    readClients.put("testCluster0", rc0);
    AvroSpecificStoreClient<ParticipantMessageKey, ParticipantMessageValue> rc1 = mock(AvroSpecificStoreClient.class);
    readClients.put("testCluster1", rc1);

    // get write clients map
    Map<String, VeniceWriter> writeClients = participantStoreClientsManager.getWriteClients();
    VeniceWriter wc0 = mock(VeniceWriter.class);
    writeClients.put("testCluster0", wc0);
    VeniceWriter wc1 = mock(VeniceWriter.class);
    writeClients.put("testCluster1", wc1);

    participantStoreClientsManager.close();

    // verify close is called on all clients
    verify(rc0).close();
    verify(rc1).close();
    verify(wc0).close();
    verify(wc1).close();
  }

  @Test
  public void testGetWriter() {
    String participantStoreRT = Utils.composeRealTimeTopic(PARTICIPANT_STORE_NAME);
    PubSubTopic participantStoreTopic = pubSubTopicRepository.getTopic(participantStoreRT);
    TopicManager localTopicManager = mock(TopicManager.class);
    when(topicManagerRepository.getLocalTopicManager()).thenReturn(localTopicManager);
    when(localTopicManager.containsTopicAndAllPartitionsAreOnline(participantStoreTopic)).thenReturn(true);

    VeniceWriter mockWriter = mock(VeniceWriter.class);
    // check RT name matches the participant store RT name
    doAnswer(invocation -> {
      VeniceWriterOptions options = invocation.getArgument(0);
      if (participantStoreRT.equals(options.getTopicName())) {
        return mockWriter;
      }
      return null;
    }).when(veniceWriterFactory).createVeniceWriter(any());

    VeniceWriter participantStoreWriter = participantStoreClientsManager.getWriter(CLUSTER_NAME);
    assertNotNull(participantStoreWriter);
    assertSame(mockWriter, participantStoreWriter);

    // call getWriter again with the same cluster name
    VeniceWriter result2 = participantStoreClientsManager.getWriter(CLUSTER_NAME);
    assertSame(participantStoreWriter, result2);
  }

  @Test
  public void testGetWriterTopicNotFound() {
    String participantStoreRT = Utils.composeRealTimeTopic(PARTICIPANT_STORE_NAME);
    PubSubTopic participantStoreTopic = pubSubTopicRepository.getTopic(participantStoreRT);
    TopicManager localTopicManager = mock(TopicManager.class);
    when(topicManagerRepository.getLocalTopicManager()).thenReturn(localTopicManager);
    when(localTopicManager.containsTopicAndAllPartitionsAreOnline(participantStoreTopic)).thenReturn(false);

    Exception exception =
        expectThrows(VeniceException.class, () -> participantStoreClientsManager.getWriter(CLUSTER_NAME));
    assertEquals(
        exception.getMessage(),
        "Can't find the expected topic " + participantStoreTopic + " for participant message store "
            + PARTICIPANT_STORE_NAME);
  }
}
