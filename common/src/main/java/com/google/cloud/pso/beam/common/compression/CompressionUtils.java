/*
 * Copyright (C) 2023 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.pso.beam.common.compression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Util functions for compression and uncompression. */
public class CompressionUtils {

  private static final Logger LOG = LoggerFactory.getLogger(CompressionUtils.class);

  /** Supported compression types. */
  public enum CompressionType {
    NO_COMPRESSION,
    THRIFT_ZLIB,
    AVRO_SNAPPY;

    public static boolean shouldDecompress(String headerValue) {
      try {
        if (headerValue == null) {
          return false;
        }
        switch (CompressionType.valueOf(headerValue)) {
          case AVRO_SNAPPY:
          case THRIFT_ZLIB:
            return true;
          default:
            return false;
        }
      } catch (Exception ex) {
        LOG.debug("Wrong compression header found " + headerValue, ex);
        return false;
      }
    }
  }

  public static final String COMPRESSION_TYPE_HEADER_KEY = "COMPRESSION_TYPE";

  public static String compressString(String srcTxt) throws IOException {
    var rstBao = new ByteArrayOutputStream();
    var zos = new GZIPOutputStream(rstBao);
    zos.write(srcTxt.getBytes());
    IOUtils.closeQuietly(zos);
    var bytes = rstBao.toByteArray();
    return Base64.getEncoder().encodeToString(bytes);
  }

  public static String uncompressString(String zippedBase64Str) throws IOException {
    String result = null;

    var bytes = Base64.getDecoder().decode(zippedBase64Str);
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
