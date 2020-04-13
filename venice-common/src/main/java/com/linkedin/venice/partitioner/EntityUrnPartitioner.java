package com.linkedin.venice.partitioner;

import com.linkedin.common.urn.Urn;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import org.apache.avro.io.DecoderFactory;
import org.apache.log4j.Logger;
import org.apache.avro.io.ByteBufferOptimizedBinaryDecoder;


/**
 * A special type of the {@link VenicePartitioner} class to extract entity URN from
 * the key and do a modulo on it to generate partition number
 *
 * Note: getPartitionId() functions make a strong assumption that the input key is a record
 * with the first field being entityUrn. If the assumption does not stand, this function
 * will NOT work properly.
 */
public class EntityUrnPartitioner extends VenicePartitioner {

  static final Logger logger = Logger.getLogger(EntityUrnPartitioner.class);

  public EntityUrnPartitioner() {
    super();
  }

  public EntityUrnPartitioner(VeniceProperties props) {
    super(props);
  }

  private int getPartitionId(byte[] keyBytes, int offset, int length, int numPartitions) {
    String entityUrn;
    try {
      // Strong assumption here the first field in this record back by keyBytes is a string type entityURN
      entityUrn = new ByteBufferOptimizedBinaryDecoder(keyBytes, offset, length).readString();
    } catch (IOException e) {
      throw new VeniceException("Unable to retrieve entityUrn from the key.", e);
    }

    int partitionId;
    try {
      partitionId = (int) (Urn.createFromString(entityUrn).getIdAsLong() % numPartitions);
    } catch (URISyntaxException e) {
      throw new VeniceException("Unable to parse entityUrn: " + entityUrn, e);
    }
    return partitionId;
  }

  @Override
  public int getPartitionId(byte[] keyBytes, int numPartitions) {
    return getPartitionId(keyBytes, 0, keyBytes.length, numPartitions);
  }

  @Override
  public int getPartitionId(ByteBuffer keyByteBuffer, int numPartitions) {
    return getPartitionId(keyByteBuffer.array(), keyByteBuffer.position(), keyByteBuffer.remaining(), numPartitions);
  }
}
