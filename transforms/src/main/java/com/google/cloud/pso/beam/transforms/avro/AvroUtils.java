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
package com.google.cloud.pso.beam.transforms.avro;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.CreateOptions;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A collection of avro related utilities. */
public class AvroUtils {

  private static final Logger LOG = LoggerFactory.getLogger(AvroUtils.class);

  /** Beam related implementation of Parquet InputFile (copied from Beam SDK). */
  public static class BeamParquetInputFile implements InputFile {

    private final SeekableByteChannel seekableByteChannel;

    public BeamParquetInputFile(SeekableByteChannel seekableByteChannel) {
      this.seekableByteChannel = seekableByteChannel;
    }

    @Override
    public long getLength() throws IOException {
      return seekableByteChannel.size();
    }

    @Override
    public SeekableInputStream newStream() {
      return new DelegatingSeekableInputStream(Channels.newInputStream(seekableByteChannel)) {

        @Override
        public long getPos() throws IOException {
          return seekableByteChannel.position();
        }

        @Override
        public void seek(long newPos) throws IOException {
          seekableByteChannel.position(newPos);
        }
      };
    }
  }

  /**
   * Given a list of original compose part files, a destination for the compose file destination,
   * and a ParquetIO.Sink instance it will read the source files and write content to the
   * destination.
   *
   * @param sink A ParquetIO.Sink initialized instance.
   * @param destinationPath the destination of the compose file
   * @param composeParts the original compose part files
   * @return True in case the compose file was successfully written, false otherwise.
   */
  public static Boolean composeParquetFiles(
      FileIO.Sink<GenericRecord> sink,
      String destinationPath,
      Iterable<FileIO.ReadableFile> composeParts) {

    LOG.debug("Writing into file {}", destinationPath);

    try (var writeChannel =
        FileSystems.create(
            FileSystems.matchNewResource(destinationPath, false),
            CreateOptions.StandardCreateOptions.builder().setMimeType("").build())) {
      sink.open(writeChannel);

      for (var readablePart : composeParts) {
        LOG.debug("Composing data from {}", readablePart.getMetadata().resourceId());
        var readerBuilder =
            AvroParquetReader.<GenericRecord>builder(
                new BeamParquetInputFile(readablePart.openSeekable()));
        try (var reader = readerBuilder.build()) {
          GenericRecord read;
          while ((read = reader.read()) != null) {
            sink.write(read);
          }
        } catch (Exception ex) {
          LOG.error("Error while composing files.", ex);
          LOG.warn(
              "Tried to compose using file {} but failed, skipping.",
              readablePart.getMetadata().resourceId().getFilename());
        }
      }
      sink.flush();
      return true;
    } catch (Exception ex) {
      LOG.error("Error while composing files.", ex);
      LOG.warn("Tried to compose into file {} but failed, skipping.", destinationPath);
      return false;
    }
  }
}
