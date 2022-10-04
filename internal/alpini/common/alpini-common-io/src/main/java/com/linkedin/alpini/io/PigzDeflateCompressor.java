package com.linkedin.alpini.io;

import java.io.IOException;
import java.util.zip.Deflater;
import javax.annotation.Nonnull;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class PigzDeflateCompressor extends PigzOutputStream.Compressor {
  private final Deflater _deflater;
  private final byte[] _inputBytes;
  private final byte[] _outputBytes;
  private int _inputBytesAvailable;
  private int _outputBytesAvailable;

  public PigzDeflateCompressor(@Nonnull PigzOutputStream pigz, int compressionLevel, int blockSize) {
    super(pigz);
    _deflater = new Deflater(compressionLevel, true);
    _inputBytes = new byte[blockSize];
    _outputBytes = new byte[12 + (int) Math.ceil(blockSize * 1.001)];
  }

  @Override
  protected void setDeflateDictionary(byte[] dict, int dictLength) {
    _deflater.setDictionary(dict, 0, dictLength);
  }

  @Override
  protected void resetDeflator() {
    _inputBytesAvailable = 0;
    _outputBytesAvailable = 0;
  }

  @Override
  protected void reinitDeflator() {
    _deflater.reset();
  }

  @Override
  protected int write(@Nonnull byte[] b, int off, int len) {
    int count = Math.min(len, _inputBytes.length - _inputBytesAvailable);
    System.arraycopy(b, off, _inputBytes, _inputBytesAvailable, count);
    _inputBytesAvailable += count;
    return count;
  }

  @Override
  protected int getInputAvailable() {
    return _inputBytesAvailable;
  }

  @Override
  protected boolean isInputBufferFull() {
    return _inputBytesAvailable == _inputBytes.length;
  }

  @Override
  @Nonnull
  protected byte[] getInputBuffer() {
    return _inputBytes;
  }

  @Override
  @Nonnull
  protected byte[] getOutputBuffer() {
    return _outputBytes;
  }

  @Override
  protected int getOutputAvailable() {
    return _outputBytesAvailable;
  }

  @Override
  protected void deflate() throws IOException {
    _deflater.setInput(getInputBuffer(), 0, getInputAvailable());
    if (isFinalBlock()) {
      _deflater.finish();
    }
    _outputBytesAvailable = _deflater.deflate(_outputBytes, 0, _outputBytes.length, Deflater.SYNC_FLUSH);
  }
}
