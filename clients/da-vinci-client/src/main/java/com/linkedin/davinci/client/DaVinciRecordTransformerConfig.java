package com.linkedin.davinci.client;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.Optional;
import org.apache.avro.Schema;


/**
 * Configuration class for {@link DaVinciRecordTransformer}, which is passed into {@link DaVinciConfig}.
 */
public class DaVinciRecordTransformerConfig {
  private final DaVinciRecordTransformerFunctionalInterface recordTransformerFunction;
  private final Class outputValueClass;
  private final Schema outputValueSchema;
  private final boolean storeRecordsInDaVinci;
  private final boolean alwaysBootstrapFromVersionTopic;

  public DaVinciRecordTransformerConfig(Builder builder) {
    this.recordTransformerFunction = Optional.ofNullable(builder.recordTransformerFunction)
        .orElseThrow(() -> new VeniceException("recordTransformerFunction cannot be null"));

    this.outputValueClass = builder.outputValueClass;
    this.outputValueSchema = builder.outputValueSchema;
    if ((this.outputValueClass != null && this.outputValueSchema == null)
        || (this.outputValueClass == null && this.outputValueSchema != null)) {
      throw new VeniceException("outputValueClass and outputValueSchema must be defined together");
    }

    this.storeRecordsInDaVinci = Optional.ofNullable(builder.storeRecordsInDaVinci).orElse(true);
    this.alwaysBootstrapFromVersionTopic = Optional.ofNullable(builder.alwaysBootstrapFromVersionTopic).orElse(false);
  }

  /**
   * @return {@link #recordTransformerFunction}
   */
  public DaVinciRecordTransformerFunctionalInterface getRecordTransformerFunction() {
    return recordTransformerFunction;
  }

  /**
   * @return {@link #outputValueClass}
   */
  public Class getOutputValueClass() {
    return outputValueClass;
  }

  /**
   * @return {@link #outputValueSchema}
   */
  public Schema getOutputValueSchema() {
    return outputValueSchema;
  }

  /**
   * @return {@link #storeRecordsInDaVinci}
   */
  public boolean getStoreRecordsInDaVinci() {
    return storeRecordsInDaVinci;
  }

  /**
   * @return {@link #alwaysBootstrapFromVersionTopic}
   */
  public boolean getAlwaysBootstrapFromVersionTopic() {
    return alwaysBootstrapFromVersionTopic;
  }

  public static class Builder {
    private DaVinciRecordTransformerFunctionalInterface recordTransformerFunction;
    private Class outputValueClass;
    private Schema outputValueSchema;
    private Boolean storeRecordsInDaVinci;
    private Boolean alwaysBootstrapFromVersionTopic;

    /**
     * @param recordTransformerFunction the functional interface for creating a {@link DaVinciRecordTransformer}
     */
    public Builder setRecordTransformerFunction(DaVinciRecordTransformerFunctionalInterface recordTransformerFunction) {
      this.recordTransformerFunction = recordTransformerFunction;
      return this;
    }

    /**
     * Set this if you modify the schema during transformation. Must be used in conjunction with {@link #setOutputValueSchema(Schema)}
     * @param outputValueClass the class of the output value
     */
    public Builder setOutputValueClass(Class outputValueClass) {
      this.outputValueClass = outputValueClass;
      return this;
    }

    /**
     * Set this if you modify the schema during transformation. Must be used in conjunction with {@link #setOutputValueClass(Class)}
     * @param outputValueSchema the schema of the output value
     */
    public Builder setOutputValueSchema(Schema outputValueSchema) {
      this.outputValueSchema = outputValueSchema;
      return this;
    }

    /**
     * @param storeRecordsInDaVinci set this to false if you intend to store records in a custom storage,
     *                              and not in the Da Vinci Client.
     *                              Default is true.
     */
    public Builder setStoreRecordsInDaVinci(boolean storeRecordsInDaVinci) {
      this.storeRecordsInDaVinci = storeRecordsInDaVinci;
      return this;
    }

    /**
     * @param alwaysBootstrapFromVersionTopic set this to true if {@link #storeRecordsInDaVinci} is false, and you're
     *                                        storing records in memory without being backed by disk.
     *                                        Default is false.
     */
    public Builder setAlwaysBootstrapFromVersionTopic(boolean alwaysBootstrapFromVersionTopic) {
      this.alwaysBootstrapFromVersionTopic = alwaysBootstrapFromVersionTopic;
      return this;
    }

    public DaVinciRecordTransformerConfig build() {
      return new DaVinciRecordTransformerConfig(this);
    }
  }
}
