package com.linkedin.venice.benchmark;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdInputStream;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.compression.ZstdWithDictCompressor;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;


@Fork(value = 2, jvmArgs = { "-Xms4G", "-Xmx4G" })
@Warmup(iterations = 2)
@Measurement(iterations = 5)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ZstdDecompressionBenchmark {
  private static final int NUMBER_OF_PAYLOADS = 10_000;
  @Param({ "500", "65536" })
  private static int PAYLOAD_SIZE;
  private VeniceCompressor compressor;
  private ByteBuffer[] compressedPayloads;
  private byte[] dictionary;
  private Random rd = new Random();

  @Setup
  public void setUp() throws Exception {
    this.dictionary = ZstdWithDictCompressor.buildDictionaryOnSyntheticAvroData();
    this.compressor = new CompressorFactory().createCompressorWithDictionary(dictionary, Zstd.maxCompressionLevel());
    this.compressedPayloads = new ByteBuffer[NUMBER_OF_PAYLOADS];
    for (int i = 0; i < NUMBER_OF_PAYLOADS; i++) {
      byte[] data = new byte[PAYLOAD_SIZE];
      this.rd.nextBytes(data);
      this.compressedPayloads[i] = compressor.compress(ByteBuffer.wrap(data), 0);
    }
  }

  @Benchmark
  @OperationsPerInvocation(NUMBER_OF_PAYLOADS)
  public void measureDecompression(org.openjdk.jmh.infra.Blackhole bh) throws IOException {
    ByteBuffer decompressed;
    ByteBuffer compressed;
    for (int i = 0; i < NUMBER_OF_PAYLOADS; i++) {
      compressed = compressedPayloads[i];
      decompressed = compressor.decompress(compressed);
      bh.consume(decompressed);
    }
  }

  @Benchmark
  @OperationsPerInvocation(NUMBER_OF_PAYLOADS)
  public void measureDecompressionWithDictionaryReload(org.openjdk.jmh.infra.Blackhole bh) throws IOException {
    ByteBuffer decompressed;
    ByteBuffer compressed;
    for (int i = 0; i < NUMBER_OF_PAYLOADS; i++) {
      compressed = compressedPayloads[i];
      try (
          InputStream bais =
              new ByteArrayInputStream(compressed.array(), compressed.position(), compressed.remaining());
          InputStream zis = new ZstdInputStream(bais).setDict(this.dictionary)) {
        decompressed = ByteBuffer.wrap(IOUtils.toByteArray(zis));
        bh.consume(decompressed);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Options opt = new OptionsBuilder().include(ZstdDecompressionBenchmark.class.getSimpleName())
        .addProfiler(GCProfiler.class)
        .build();
    new Runner(opt).run();
  }
}
