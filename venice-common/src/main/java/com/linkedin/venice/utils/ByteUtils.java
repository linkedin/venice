package com.linkedin.venice.utils;

import org.apache.commons.codec.binary.Hex;

public class ByteUtils {
    /**
     * Translate the given byte array into a hexidecimal string
     *
     * @param bytes The bytes to translate
     * @return The string
     */
    public static String toHexString(byte[] bytes) {
        return Hex.encodeHexString(bytes);
    }
}
