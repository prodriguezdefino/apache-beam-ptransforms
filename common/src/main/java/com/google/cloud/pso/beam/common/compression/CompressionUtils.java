package com.google.cloud.pso.beam.common.compression;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.compress.utils.IOUtils;


/**
 * Util functions for compression and uncompression.
 */
public class CompressionUtils {

  public static String compressString(String srcTxt)
          throws IOException {
    ByteArrayOutputStream rstBao = new ByteArrayOutputStream();
    GZIPOutputStream zos = new GZIPOutputStream(rstBao);
    zos.write(srcTxt.getBytes());
    IOUtils.closeQuietly(zos);
    byte[] bytes = rstBao.toByteArray();
    return Base64.getEncoder().encodeToString(bytes);
  }

  public static String uncompressString(String zippedBase64Str)
          throws IOException {
    String result = null;

    byte[] bytes = Base64.getDecoder().decode(zippedBase64Str);
    GZIPInputStream zi = null;
    try {
      zi = new GZIPInputStream(new ByteArrayInputStream(bytes));
      result = new String(IOUtils.toByteArray(zi));
    } finally {
      IOUtils.closeQuietly(zi);
    }
    return result;
  }
}
