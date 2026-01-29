package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pushmonitor.KillOfflinePushMessage;
import com.linkedin.venice.stats.HelixMessageChannelStats;
import com.linkedin.venice.status.StatusMessage;
import com.linkedin.venice.status.StatusMessageChannel;
import com.linkedin.venice.status.StatusMessageHandler;
import com.linkedin.venice.status.StoreStatusMessage;
import com.linkedin.venice.utils.Utils;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.helix.ClusterMessagingService;
import org.apache.helix.Criteria;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.messaging.AsyncCallback;
import org.apache.helix.messaging.handling.HelixTaskResult;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.model.Message;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The control message changed built on Helix message service.
 * <p>
 * It will convert Venice message to helix message when sending and reverse this process when receiving.
 * <p>
 * Only one Helix message type is used, so channel is similar to a dispatcher that receive all of control messages and
 * dispatch them to related handlers.
 */
@Deprecated
public class HelixStatusMessageChannel implements StatusMessageChannel {
  private static final Logger LOGGER = LogManager.getLogger(HelixStatusMessageChannel.class);

  public static final int DEFAULT_SEND_MESSAGE_TIME_OUT = 1000;

  public static final String HELIX_MESSAGE_TYPE = "control_message";

  public static final String VENICE_MESSAGE_CLASS = "veniceMessageClass";

  public static final String VENICE_MESSAGE_FIELD = "veniceMessageFields";

  private final ClusterMessagingService messageService;

  private final Map<String, StatusMessageHandler> handlers = new ConcurrentHashMap<>();

  private final int sendMessageTimeOut;

  private final HelixMessageChannelStats stats;

  public HelixStatusMessageChannel(SafeHelixManager manager, HelixMessageChannelStats stats) {
    this(manager, stats, DEFAULT_SEND_MESSAGE_TIME_OUT);
  }

  public HelixStatusMessageChannel(SafeHelixManager manager, HelixMessageChannelStats stats, int timeOut) {
    messageService = manager.getMessagingService();
    this.stats = stats;
    this.sendMessageTimeOut = timeOut;
    messageService.registerMessageHandlerFactory(HELIX_MESSAGE_TYPE, new HelixStatusMessageHandleFactory());
  }

  @Override
  public void sendToController(StatusMessage message, int retryCount, long retryDurationMs) {
    Message helixMessage = convertVeniceMessageToHelixMessage(message);
    // TODO will confirm with Helix team that do we need to specify session Id here.
    // TODO If we assign a session Id of participant here, when this session be expired/changed by any reason,
    // TODO the message will be ignored by controller. SO use arbitrary value here at first.
    helixMessage.setTgtSessionId("*");
    Criteria criteria = new Criteria();
    criteria.setRecipientInstanceType(InstanceType.CONTROLLER);
    criteria.setSessionSpecific(false);

    boolean isSuccess = false;
    int attempt = 0;
    while (!isSuccess && attempt <= retryCount) {
      attempt++;
      if (attempt > 1) {
        // only wait and print the log of the retry.
        LOGGER.info("Wait {}ms to retry.", retryDurationMs);
        Utils.sleep(retryDurationMs);
        LOGGER.info("Attempt #{}: Sending message to controller.", attempt);
        // Use a new message Id, otherwise controller will not handle the retry message because it think it had already
        // failed.
        // It a new issue introduced by new helix version 0.6.6.1
        helixMessage.setMsgId(StatusMessage.generateMessageId());
      }
      try {
        ControlMessageCallback callBack = new ControlMessageCallback();
        // Send and wait until getting response or time out.
        int numMsg = messageService.sendAndWait(criteria, helixMessage, callBack, sendMessageTimeOut);
        if (numMsg == 0) {
          LOGGER.error("No controller could be found to send messages {}.", message.getMessageId());
          continue;
        }
        if (callBack.isTimeOut) {
          LOGGER.error("Error: Can not send message to controller. Sending is time out.");
          continue;
        }
        Message replyMessage = callBack.getMessageReplied().get(0);
        String result = replyMessage.getResultMap().get("SUCCESS");
        isSuccess = Boolean.valueOf(result);
        if (!isSuccess) {
          LOGGER.error(
              "Error: controller can not handle this message correctly. "
                  + replyMessage.getResultMap().get("ERRORINFO"));
        }
        continue;
      } catch (Exception e) {
        LOGGER.error("Error: Can not send message to controller.", e);
        continue;
      }
    }

    if (!isSuccess) {
      String errorMsg = "Error: After attempting " + attempt + " times, sending is still failed.";
      LOGGER.error(errorMsg);
      throw new VeniceException(errorMsg);
    }
  }

  @Override
  public void sendToController(StatusMessage message) {
    this.sendToController(message, 0, 0);
  }

  @Override
  public void sendToStorageNodes(String clusterName, StatusMessage message, String resourceName, int retryCount) {
    stats.recordToStorageNodesInvokeCount();
    Message helixMessage = convertVeniceMessageToHelixMessage(message);
    helixMessage.setTgtSessionId("*");
    Criteria criteria = new Criteria();
    criteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
    criteria.setSessionSpecific(false);
    // Broad messages to all alive storage nodes.
    criteria.setDataSource(Criteria.DataSource.LIVEINSTANCES);
    criteria.setInstanceName("%");
    criteria.setClusterName(clusterName);

    ControlMessageCallback callBack = new ControlMessageCallback();
    // Send and wait until getting response or time out.
    int numMsgSent = messageService.sendAndWait(criteria, helixMessage, callBack, sendMessageTimeOut, retryCount);
    if (numMsgSent == 0) {
      LOGGER.error("No storage node is found to send message to. Message: {}", message);
      // Not throwing exception for now,
      // this scenario could be introduced by two cases: wrong resource or no storage node.
      // TODO: need to think a better way to handle those two different scenarios.
    } else {
      LOGGER.info("Sent {} messages to storage nodes. Message: {}.", numMsgSent, message);
    }

    stats.recordToStorageNodesMessageCount(numMsgSent);
    stats.recordMissedStorageNodesReplyCount(numMsgSent - callBack.getMessageReplied().size());

    if (callBack.isTimeOut) {
      String errorMsg = "Sending messages to storage node is time out. Resource:" + resourceName + ". Message sent:"
          + numMsgSent + ". Message replied:" + callBack.getMessageReplied().size();
      LOGGER.error(errorMsg);
      throw new VeniceException(errorMsg);
    }

    boolean isSuccessful = true;
    for (Message replyMessage: callBack.getMessageReplied()) {
      String result = replyMessage.getResultMap().get("SUCCESS");
      if (!Boolean.valueOf(result)) {
        // message is not processed by storage node successfully.
        LOGGER.error("Message is not processed successfully by instance: {}.", replyMessage.getMsgSrc());
        isSuccessful = false;
      }
    }
    if (isSuccessful) {
      LOGGER.info("{} messages have been send and processed. Message: {}.", numMsgSent, message);
    } else {
      // We have printed the detail error information before for each of message.
      throw new VeniceException("Some of storage node did not process message successfully. Message:" + message);
    }
  }

  @Override
  public <T extends StatusMessage> void registerHandler(Class<T> clazz, StatusMessageHandler<T> handler) {
    if (this.handlers.containsKey(clazz.getName())) {
      throw new VeniceException("Handler already exists for message type:" + clazz.getName());
    }
    this.handlers.put(clazz.getName(), handler);
  }

  @Override
  public <T extends StatusMessage> void unRegisterHandler(Class<T> clazz, StatusMessageHandler<T> handler) {
    if (!handlers.containsKey(clazz.getName())) {
      // If no listener is found by given class, just skip this un-register request.
      LOGGER.info("Can not find any handler for given message type: {}.", clazz.toGenericString());
      return;
    }
    if (handler.equals(this.handlers.get(clazz.getName()))) {
      this.handlers.remove(clazz.getName());
    } else {
      throw new VeniceException("Handler is different from the registered one. Message type:" + clazz.getName());
    }
  }

  /**
   * Convert Helix message to Venice Message.
   *
   * @param helixMessage
   *
   * @return
   */
  protected StatusMessage convertHelixMessageToVeniceMessage(Message helixMessage) {
    String className = helixMessage.getRecord().getSimpleField(VENICE_MESSAGE_CLASS);
    Map<String, String> fields = helixMessage.getRecord().getMapField(VENICE_MESSAGE_FIELD);

    if (StoreStatusMessage.class.getName().equals(className)) {
      return new StoreStatusMessage(fields);
    } else if (KillOfflinePushMessage.class.getName().equals(className)) {
      return new KillOfflinePushMessage(fields);
    } else {
      throw new VeniceException("Message handler not implemented yet for class." + className);
    }
  }

  /**
   * Convert Venice message to Helix message.
   *
   * @param veniceMessage
   *
   * @return
   */
  protected Message convertVeniceMessageToHelixMessage(StatusMessage veniceMessage) {
    Message helixMessage = new Message(HELIX_MESSAGE_TYPE, veniceMessage.getMessageId());
    helixMessage.getRecord().setMapField(VENICE_MESSAGE_FIELD, veniceMessage.getFields());
    helixMessage.getRecord().setSimpleField(VENICE_MESSAGE_CLASS, veniceMessage.getClass().getName());
    return helixMessage;
  }

  /**
   * Get the handler for given Venice message type.
   *
   * @param clazz
   * @param <T>
   *
   * @return
   */
  protected <T extends StatusMessage> StatusMessageHandler getHandler(Class<T> clazz) {
    StatusMessageHandler handler = handlers.get(clazz.getName());
    if (handler == null) {
      throw new VeniceException("No handler for this type of message:" + clazz.getName());
    } else {
      return handler;
    }
  }

  /**
   * Helix message handler factory to create handler to deal with Helix message.
   */
  private class HelixStatusMessageHandleFactory implements MessageHandlerFactory {
    @Override
    public MessageHandler createHandler(Message message, NotificationContext context) {
      if (message.getMsgType().equals(HELIX_MESSAGE_TYPE)) {
        return new HelixStatusMessageHandler(message, context);
      } else {
        throw new VeniceException(
            "Unexpected message type:" + message.getMsgType() + " for message:" + message.getMsgId());
      }
    }

    @Override
    public String getMessageType() {
      return HELIX_MESSAGE_TYPE;
    }

    @Override
    public void reset() {
      // Ignore. We don't need reset in this factory.
    }
  }

  /**
   * Helix message handler used to deal with all of control messages.
   */
  private class HelixStatusMessageHandler extends MessageHandler {
    /**
     * The constructor. The message and notification context must be provided via creation.
     *
     * @param message
     * @param context
     */
    public HelixStatusMessageHandler(Message message, NotificationContext context) {
      super(message, context);
    }

    @Override
    public HelixTaskResult handleMessage() {
      StatusMessage msg = convertHelixMessageToVeniceMessage(_message);
      HelixTaskResult result = new HelixTaskResult();
      // Dispatch venice message to related hander.
      // Do no need to handle exception here, Helix will help to catch and set success of result to false.
      try {
        getHandler(msg.getClass()).handleMessage(msg);
      } catch (Exception e) {
        // Log the message content in case of handing failure
        LOGGER.error("Handle message {} failed. Venice message content: {}.", _message.getId(), msg, e);
        // re-throw exception to helix.
        throw e;
      }
      result.setSuccess(true);
      return result;
    }

    @Override
    public void onError(Exception e, ErrorCode code, ErrorType type) {
      LOGGER.error("Message handling pipeline met error for message: {}.", _message.getMsgId(), e);
    }
  }

  /**
   * Call back used by Helix message service. Only used to check that is sending timeout or not here.
   */
  private class ControlMessageCallback extends AsyncCallback {
    private boolean isTimeOut = false;

    @Override
    public void onTimeOut() {
      isTimeOut = true;
    }

    @Override
    public void onReplyMessage(Message message) {
      // ignore
      stats.recordOnReplyFromStorageNodesCount();
    }
  }
}
