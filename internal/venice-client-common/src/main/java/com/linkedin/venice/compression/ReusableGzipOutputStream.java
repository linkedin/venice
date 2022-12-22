package com.linkedin.venice.compression;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;


/**
 * This class implements a stream filter for writing compressed data in the GZIP file format. It's an adoption of
 * {@link java.util.zip.GZIPOutputStream} but with a notable difference regarding re-usability:
 * <ul>
 * <li>Expose {@link #reset()} to reset CRC32 and the deflater</li>
 * <li>Don't write the GZIP header upon construction but expose {@link #writeHeader()}</li>
 * </ul>
 */
public class ReusableGzipOutputStream extends DeflaterOutputStream {
  /**
   * CRC-32 of uncompressed data.
   */
  private final CRC32 crc = new CRC32();

  /*
   * GZIP header magic number.
   */
  private final static int GZIP_MAGIC = 0x8b1f;

  /*
   * Trailer size in bytes.
   *
   */
  private final static int TRAILER_SIZE = 8;

  /**
   * Creates a new output stream with the specified buffer size and flush mode.
   *
   * @param out the output stream
   * @exception IllegalArgumentException if {@code size <= 0}
   *
   * @since 1.7
   */
  private final ByteArrayOutputStream bos;

  public ReusableGzipOutputStream(ByteArrayOutputStream out) {
    super(out, new Deflater(Deflater.DEFAULT_COMPRESSION, true));
    crc.reset();
    out.reset();
    this.bos = out;
  }

  public byte[] toByteArray() {
    return bos.toByteArray();
  }

  /**
   * Writes array of bytes to the compressed output stream. This method will block until all the bytes are written.
   *
   * @param buf the data to be written
   * @param off the start offset of the data
   * @param len the length of the data
   * @exception IOException If an I/O error has occurred.
   */
  public synchronized void write(byte[] buf, int off, int len) throws IOException {
    super.write(buf, off, len);
    crc.update(buf, off, len);
  }

  /**
   * Finishes writing compressed data to the output stream without closing the underlying stream. Use this method when
   * applying multiple filters in succession to the same output stream.
   *
   * @exception IOException if an I/O error has occurred
   */
  public void finish() throws IOException {
    if (!def.finished()) {
      def.finish();
      while (!def.finished()) {
        int len = def.deflate(buf, 0, buf.length);
        if (def.finished() && len <= buf.length - TRAILER_SIZE) {
          // last deflater buffer. Fit trailer at the end
          writeTrailer(buf, len);
          len = len + TRAILER_SIZE;
          out.write(buf, 0, len);
          return;
        }
        if (len > 0)
          out.write(buf, 0, len);
      }
      // if we can't fit the trailer at the end of the last
      // deflater buffer, we write it separately
      byte[] trailer = new byte[TRAILER_SIZE];
      writeTrailer(trailer, 0);
      out.write(trailer);
    }
  }

  /*
   * Writes GZIP member header.
   */
  public void writeHeader() throws IOException {
    out.write(
        new byte[] { (byte) GZIP_MAGIC, // Magic number (short)
            (byte) (GZIP_MAGIC >> 8), // Magic number (short)
            Deflater.DEFLATED, // Compression method (CM)
            0, // Flags (FLG)
            0, // Modification time MTIME (int)
            0, // Modification time MTIME (int)
            0, // Modification time MTIME (int)
            0, // Modification time MTIME (int)
            0, // Extra flags (XFLG)
            0 // Operating system (OS)
        });
  }

  /*
   * Writes GZIP member trailer to a byte array, starting at a given offset.
   */
  private void writeTrailer(byte[] buf, int offset) throws IOException {
    writeInt((int) crc.getValue(), buf, offset); // CRC-32 of uncompr. data
    writeInt(def.getTotalIn(), buf, offset + 4); // Number of uncompr. bytes
  }

  /*
   * Writes integer in Intel byte order to a byte array, starting at a given offset.
   */
  private void writeInt(int i, byte[] buf, int offset) throws IOException {
    writeShort(i & 0xffff, buf, offset);
    writeShort((i >> 16) & 0xffff, buf, offset + 2);
  }

  /*
   * Writes short integer in Intel byte order to a byte array, starting at a given offset
   */
  private void writeShort(int s, byte[] buf, int offset) throws IOException {
    buf[offset] = (byte) (s & 0xff);
    buf[offset + 1] = (byte) ((s >> 8) & 0xff);
  }

  public void reset() {
    crc.reset();
    def.reset();
    bos.reset();
  }
}
