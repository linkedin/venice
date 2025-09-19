package com.linkedin.davinci.client;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.lazy.Lazy;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;


/**
 * Configuration for {@link DaVinciRecordTransformer}, supplied via {@link DaVinciConfig#setRecordTransformerConfig}.
 *
 * Required: set {@link Builder#setRecordTransformerFunction(DaVinciRecordTransformerFunctionalInterface)} to register your callbacks.
 *
 * See optional configs in the {@link Builder} section below.
 */
public class DaVinciRecordTransformerConfig {
  private final DaVinciRecordTransformerFunctionalInterface recordTransformerFunction;
  private final Class keyClass;
  private final Class outputValueClass;
  private final Schema outputValueSchema;
  private final boolean storeRecordsInDaVinci;
  private final boolean alwaysBootstrapFromVersionTopic;
  private final boolean skipCompatibilityChecks;
  private final boolean useSpecificRecordKeyDeserializer;
  private final boolean useSpecificRecordValueDeserializer;

  public DaVinciRecordTransformerConfig(Builder builder) {
    this.recordTransformerFunction = Optional.ofNullable(builder.recordTransformerFunction)
        .orElseThrow(() -> new VeniceException("recordTransformerFunction cannot be null"));

    this.keyClass = builder.keyClass;
    this.outputValueClass = builder.outputValueClass;
    this.outputValueSchema = builder.outputValueSchema;
    if ((this.outputValueClass != null && this.outputValueSchema == null)
        || (this.outputValueClass == null && this.outputValueSchema != null)) {
      throw new VeniceException("outputValueClass and outputValueSchema must be defined together");
    }

    this.useSpecificRecordKeyDeserializer = keyClass != null && SpecificRecord.class.isAssignableFrom(keyClass);
    this.useSpecificRecordValueDeserializer =
        outputValueClass != null && SpecificRecord.class.isAssignableFrom(outputValueClass);

    this.storeRecordsInDaVinci = builder.storeRecordsInDaVinci;
    this.alwaysBootstrapFromVersionTopic = builder.alwaysBootstrapFromVersionTopic;
    this.skipCompatibilityChecks = builder.skipCompatibilityChecks;
  }

  /**
   * @return {@link #recordTransformerFunction}
   */
  public DaVinciRecordTransformerFunctionalInterface getRecordTransformerFunction() {
    return recordTransformerFunction;
  }

  /**
   * @return {@link #keyClass}
   */
  public Class getKeyClass() {
    return keyClass;
  }

  /**
   * @return Whether the {@link SpecificRecord} deserializer should be used for keys
   */
  public boolean useSpecificRecordKeyDeserializer() {
    return useSpecificRecordKeyDeserializer;
  }

  /**
   * @return {@link #outputValueClass}
   */
  public Class getOutputValueClass() {
    return outputValueClass;
  }

  /**
   * @return Whether the {@link SpecificRecord} deserializer should be used for values
   */
  public boolean useSpecificRecordValueDeserializer() {
    return useSpecificRecordValueDeserializer;
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

  /**
   * @return {@link #skipCompatibilityChecks}
   */
  public boolean shouldSkipCompatibilityChecks() {
    return skipCompatibilityChecks;
  }

  public static class Builder {
    private DaVinciRecordTransformerFunctionalInterface recordTransformerFunction;
    private Class keyClass;
    private Class outputValueClass;
    private Schema outputValueSchema;
    private Boolean storeRecordsInDaVinci = true;
    private Boolean alwaysBootstrapFromVersionTopic = false;
    private Boolean skipCompatibilityChecks = false;

    /**
     * Required for creating a {@link DaVinciRecordTransformer}. The function is invoked with the store version at startup.
     *
     * @param recordTransformerFunction functional interface that constructs the transformer
     */
    public Builder setRecordTransformerFunction(DaVinciRecordTransformerFunctionalInterface recordTransformerFunction) {
      this.recordTransformerFunction = recordTransformerFunction;
      return this;
    }

    /**
     * Optional. Set this if you want to deserialize keys into {@link org.apache.avro.specific.SpecificRecord}.
     *
     * @param keyClass the class of the key
     */
    public Builder setKeyClass(Class keyClass) {
      this.keyClass = keyClass;
      return this;
    }

    /**
     * Optional. Set when you change the value type/schema or when values should be deserialized into
     * {@link org.apache.avro.specific.SpecificRecord}. Must be used with {@link #setOutputValueSchema(Schema)}.
     *
     * @param outputValueClass the class of the output value
     */
    public Builder setOutputValueClass(Class outputValueClass) {
      this.outputValueClass = outputValueClass;
      return this;
    }

    /**
     * Optional. Set when you change the value type/schema or when values should be deserialized into
     * {@link org.apache.avro.specific.SpecificRecord}. Must be used with {@link #setOutputValueClass(Class)}.
     *
     * @param outputValueSchema the schema of the output value
     */
    public Builder setOutputValueSchema(Schema outputValueSchema) {
      this.outputValueSchema = outputValueSchema;
      return this;
    }

    /**
     * Control whether records are persisted into Da Vinci's local disk. Set to false to route writes only
     * to your own storage via transformer callback.
     * 
     * It's not recommended to set this to false, as you will not be able to leverage blob transfer, impacting bootstrapping time.
     * 
     * Default is true.
     *
     * @param storeRecordsInDaVinci whether to store records in Da Vinci
     */
    public Builder setStoreRecordsInDaVinci(boolean storeRecordsInDaVinci) {
      this.storeRecordsInDaVinci = storeRecordsInDaVinci;
      return this;
    }

    /**
     * Set this to true if {@link #storeRecordsInDaVinci} is false, and you're storing records in memory without being backed by disk.
     * 
     * Default is false.
     *
     * @param alwaysBootstrapFromVersionTopic whether to always bootstrap from the Version Topic
     */
    public Builder setAlwaysBootstrapFromVersionTopic(boolean alwaysBootstrapFromVersionTopic) {
      this.alwaysBootstrapFromVersionTopic = alwaysBootstrapFromVersionTopic;
      return this;
    }

    /**
     * Skip transform compatibility checks. Consider true if {@link DaVinciRecordTransformer#transform(Lazy, Lazy, int)}
     * always returns UNCHANGED, or during rapid non-functional iterations when you want to avoid wiping local data on
     * redeploy. Remember to set this back to false when stable.
     * 
     * Default is false.
     *
     * @param skipCompatibilityChecks whether to skip compatibility checks
     */
    public Builder setSkipCompatibilityChecks(boolean skipCompatibilityChecks) {
      this.skipCompatibilityChecks = skipCompatibilityChecks;
      return this;
    }

    public DaVinciRecordTransformerConfig build() {
      return new DaVinciRecordTransformerConfig(this);
    }
  }
}
