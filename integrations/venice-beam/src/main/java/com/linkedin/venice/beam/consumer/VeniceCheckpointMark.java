package com.linkedin.venice.beam.consumer;

import com.linkedin.davinci.consumer.VeniceChangeCoordinate;
import com.linkedin.venice.pubsub.PubSubPositionDeserializer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceCheckpointMark implements UnboundedSource.CheckpointMark {
  private static final Logger LOG = LogManager.getLogger(VeniceCheckpointMark.class);

  private final List<VeniceChangeCoordinate> _veniceChangeCoordinates;

  VeniceCheckpointMark(List<VeniceChangeCoordinate> veniceChangeCoordinates) {
    this._veniceChangeCoordinates = veniceChangeCoordinates;
  }

  @Override
  public void finalizeCheckpoint() {
    // do Nothing!
  }

  public List<VeniceChangeCoordinate> getVeniceChangeCoordinates() {
    return _veniceChangeCoordinates;
  }

  public static class Coder extends AtomicCoder<VeniceCheckpointMark> {
    private static final long serialVersionUID = 1L;
    private final VarIntCoder _sizeCoder = VarIntCoder.of();
    private final StringUtf8Coder _stringUtf8Coder = StringUtf8Coder.of();
    private final transient PubSubPositionDeserializer pubSubPositionDeserializer;

    public Coder(PubSubPositionDeserializer positionDeserializer) {
      this.pubSubPositionDeserializer = positionDeserializer;
    }

    @Override
    public void encode(VeniceCheckpointMark value, OutputStream outStream) throws IOException {
      LOG.debug("Encoding {} veniceChangeCoordinates", value.getVeniceChangeCoordinates().size());
      _sizeCoder.encode(value.getVeniceChangeCoordinates().size(), outStream);
      value.getVeniceChangeCoordinates().forEach(veniceChangeCoordinate -> {
        try {
          String encodeString =
              VeniceChangeCoordinate.convertVeniceChangeCoordinateToStringAndEncode(veniceChangeCoordinate);
          _stringUtf8Coder.encode(encodeString, outStream);
        } catch (IOException e) {
          throw new IllegalArgumentException(e);
        }
      });
    }

    @Override
    public VeniceCheckpointMark decode(InputStream inStream) throws IOException {
      int listSize = _sizeCoder.decode(inStream);
      LOG.info("Decoding {} veniceChangeCoordinates", listSize);
      List<VeniceChangeCoordinate> veniceChangeCoordinates = new ArrayList<>(listSize);
      for (int i = 0; i < listSize; i++) {
        String decodedString = _stringUtf8Coder.decode(inStream);
        try {
          /**
           * For now, we pass the default deserializer to decode the string. This is because we don't have
           * a way to pass the deserializer to the coder. In the future, we can add a constructor to the coder
           */
          VeniceChangeCoordinate veniceChangeCoordinate = VeniceChangeCoordinate
              .decodeStringAndConvertToVeniceChangeCoordinate(pubSubPositionDeserializer, decodedString);
          veniceChangeCoordinates.add(veniceChangeCoordinate);
        } catch (ClassNotFoundException e) {
          throw new IllegalArgumentException(e);
        }
      }
      return new VeniceCheckpointMark(veniceChangeCoordinates);
    }
  }
}
