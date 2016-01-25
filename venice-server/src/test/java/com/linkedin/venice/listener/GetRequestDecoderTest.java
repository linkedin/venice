package com.linkedin.venice.listener;

import com.linkedin.venice.message.GetRequestObject;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by mwise on 1/20/16.
 */
public class GetRequestDecoderTest {

  @Test
  public void testIncompletePacketNotRead()
      throws Exception {
    String topic = "test-topic";  //10 bytes
    byte[] key = "test-key".getBytes(); // 8 bytes
    int partition = 0;

    GetRequestObject request = new GetRequestObject(topic.toCharArray(), partition, key);

    GetRequestDecoder decoder = new GetRequestDecoder();
    ByteBuf buffer = Unpooled.buffer(64);
    buffer.writeBytes(request.serialize(), 0, 20); //complete header, incomplete payload
    List<Object> out = new ArrayList<Object>();

    Assert.assertEquals(0, out.size());
    decoder.decode(null, buffer, out);
    Assert.assertEquals(0, out.size());
  }

  @Test
  public void testCompletePacketIsRead()
      throws Exception {
    String topic = "test-topic";  //10 bytes
    byte[] key = "test-key".getBytes(); // 8 bytes
    int partition = 12;

    GetRequestObject request = new GetRequestObject(topic.toCharArray(), partition, key);

    GetRequestDecoder decoder = new GetRequestDecoder();
    ByteBuf buffer = Unpooled.buffer(64);
    buffer.writeBytes(request.serialize());
    List<Object> out = new ArrayList<Object>();

    Assert.assertEquals(0, out.size());
    decoder.decode(null, buffer, out);
    Assert.assertEquals(1, out.size());

    GetRequestObject deserialized = (GetRequestObject) out.get(0);
    Assert.assertEquals(topic, deserialized.getTopicString());
    Assert.assertEquals(partition, deserialized.getPartition());
    Assert.assertEquals(key, deserialized.getKey());

  }
}
