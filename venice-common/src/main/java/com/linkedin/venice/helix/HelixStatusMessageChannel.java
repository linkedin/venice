package com.linkedin.venice.helix;

import com.linkedin.venice.status.StatusMessage;
import com.linkedin.venice.status.StatusMessageChannel;
import com.linkedin.venice.status.StatusMessageHandler;
import com.linkedin.venice.status.StoreStatusMessage;
import com.linkedin.venice.exceptions.VeniceException;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.helix.ClusterMessagingService;
import org.apache.helix.Criteria;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.messaging.AsyncCallback;
import org.apache.helix.messaging.handling.HelixTaskResult;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.model.Message;
import org.apache.log4j.Logger;


/**
 * The control message changed built on Helix message service.
 * <p>
 * It will convert Venice message to helix message when sending and reverse this process when receiving.
 * <p>
 * Only one Helix message type is used, so channel is similar to a dispatcher that receive all of control messages and
 * dispatch them to related handlers.
 */
public class HelixStatusMessageChannel implements StatusMessageChannel {
  private static final Logger logger = Logger.getLogger(HelixStatusMessageChannel.class);

  public static final int WAIT_TIME_OUT = 1000;

  public static final int RETRY_COUNT = 1;

  public static final String HELIX_MESSAGE_TYPE = "control_message";

  public static final String VENICE_MESSAGE_CLASS = "veniceMessageClass";

  public static final String VENICE_MESSAGE_FIELD = "veniceMessageFields";

  private final ClusterMessagingService messageService;

  private final Map<String, StatusMessageHandler> handlers = new ConcurrentHashMap<>();

  private final int timeOut;

  private final int retryCount;

  public HelixStatusMessageChannel(HelixManager manager) {
    this(manager, WAIT_TIME_OUT, RETRY_COUNT);
  }

  public HelixStatusMessageChannel(HelixManager manager, int timeOut, int retryCount) {
    messageService = manager.getMessagingService();
    this.timeOut = timeOut;
    this.retryCount = retryCount;
    messageService.registerMessageHandlerFactory(HELIX_MESSAGE_TYPE, new HelixControlMessageHandleFactory());
  }

  @Override
  public void sendToController(StatusMessage message)
      throws IOException {
    Message helixMessage = convertVeniceMessageToHelixMessage(message);
    //TODO will confirm with Helix team that do we need to specify session Id here.
    //TODO If we assign a session Id of participant here, when this session be expired/changed by any reason,
    //TODO the message will be ignored by controller. SO use arbitrary value here at first.
    helixMessage.setTgtSessionId("*");
    Criteria criteria = new Criteria();
    criteria.setRecipientInstanceType(InstanceType.CONTROLLER);
    criteria.setSessionSpecific(false);
    ControlMessageCallback callBack = new ControlMessageCallback();
    try {
      //Send and wait until getting response or time out.
      int numMsg = messageService.sendAndWait(criteria, helixMessage, callBack, timeOut, retryCount);
      if (numMsg == 0) {
        throw new VeniceException("No controller could be found to send messages " + message.getMessageId());
      }
    } catch (Exception e) {
      throw new IOException("Error: Can not send message to controller.", e);
    }
    if (callBack.isTimeOut) {
      throw new IOException("Error: Can not send message to controller. Sending is time out.");
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
      logger.info("Can not find any handler for given message type:" + clazz.toGenericString());
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

      if(StoreStatusMessage.class.getName().equals(className)) {
        return new StoreStatusMessage(fields);
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
  private class HelixControlMessageHandleFactory implements MessageHandlerFactory {

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
      //Ignore. We don't need reset in this factory.
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
    public HelixTaskResult handleMessage()
        throws InterruptedException {
      StatusMessage msg = convertHelixMessageToVeniceMessage(_message);
      HelixTaskResult result = new HelixTaskResult();
      try {
        //Dispatch venice message to related hander.
        getHandler(msg.getClass()).handleMessage(msg);
        result.setSuccess(true);
      } catch (Throwable e) {
        logger.error("Handle message:" + _message.getMsgId() + " failed.", e);
        result.setSuccess(false);
      }
      //TODO could put more information to result here.
      return result;
    }

    @Override
    public void onError(Exception e, ErrorCode code, ErrorType type) {
      logger.error("Message handling pipeline met error for message:" + _message.getMsgId(), e);
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
      //ignore
    }
  }
}
