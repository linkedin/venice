package org.apache.avro.io;

import com.linkedin.venice.utils.ByteUtils;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import org.apache.avro.Schema;
import org.apache.avro.io.parsing.Symbol;


/**
 * This class overrides {@link JsonEncoder#writeBytes(byte[], int, int)}, in order to convert
 * all bytes schema data to hexadecimal strings in the output stream, since hexadecimal strings
 * are more readable.
 *
 * This should be used in tooling only.
 */
public class ByteBufferToHexFormatJsonEncoder extends JsonEncoder {
  private CharsetEncoder USASCIIEncoder = StandardCharsets.US_ASCII.newEncoder();

  public ByteBufferToHexFormatJsonEncoder(Schema sc, OutputStream out) throws IOException {
    super(sc, out);
  }

  @Override
  public void writeBytes(byte[] bytes, int start, int len) throws IOException {
    parser.advance(Symbol.BYTES);
    /**
     * Explicitly push a STRING symbol into parser since {@link JsonEncoder#writeString(String)} will try to
     * pop a STRING symbol.
     *
     * Manually adding a STRING symbol is inevitable, because the JsonGenerator in {@link JsonEncoder} is a private
     * variable, so we can't put whatever we want into the output; we can only use public or protected functions which
     * always update the parser.
     */
    parser.pushSymbol(Symbol.STRING);
    writeString(ByteUtils.toHexString(bytes, start, len));
  }

  /**
   * Detect whether there is any non-ASCII character; if so, transfer the entire string into hexadecimal string.
   */
  @Override
  public void writeString(String str) throws IOException {
    if (USASCIIEncoder.canEncode(str)) {
      super.writeString(str);
    } else {
      super.writeString(ByteUtils.toHexString(str.getBytes(StandardCharsets.UTF_8)));
    }
  }
}
