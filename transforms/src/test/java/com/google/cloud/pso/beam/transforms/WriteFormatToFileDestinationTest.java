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
package com.google.cloud.pso.beam.transforms;

import com.google.cloud.pso.beam.common.Utilities;
import com.google.cloud.pso.beam.transforms.avro.AvroUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.avro.coders.AvroGenericCoder;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** */
public class WriteFormatToFileDestinationTest {

  @Rule public transient TestPipeline testPipeline = TestPipeline.create();
  @Rule public transient TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testParquetWrite() throws IOException {
    setupPipeline(WriteFormatToFileDestination.<GenericRecord>create().withNumShards(1), 100);

    testPipeline.run().waitUntilFinish();

    // create the context object needed for the DoFn test
    List<String> resourceList =
        Files.walk(temporaryFolder.getRoot().toPath())
            .filter(Files::isRegularFile)
            .map(p -> p.toAbsolutePath().toString())
            .collect(Collectors.toList());

    // there has been files written
    Assert.assertTrue(!resourceList.isEmpty());
    // there is a success file
    Assert.assertTrue(resourceList.stream().anyMatch(f -> f.endsWith("SUCCESS")));
    // there are more files with data
    Assert.assertTrue(resourceList.size() > 1);
  }

  @Test
  public void testParquetComposeWrite() throws IOException {

    String outputPath = temporaryFolder.getRoot().getAbsolutePath() + '/';

    setupPipeline(
        WriteFormatToFileDestination.<GenericRecord>create()
            .withNumShards(2)
            // we expect only one output file
            .withComposeShards(1)
            .withComposeSmallFiles(true)
            .withComposeTempDirectory(
                ValueProvider.StaticValueProvider.of(outputPath + "temp/compose"))
            .withComposeFunction(AvroUtils::composeParquetFiles),
        100);

    testPipeline.run().waitUntilFinish();

    // create the context object needed for the DoFn test
    List<String> resourceList =
        Files.walk(temporaryFolder.getRoot().toPath())
            .filter(Files::isRegularFile)
            .map(p -> p.toAbsolutePath().toString())
            .collect(Collectors.toList());

    // there has been files written
    Assert.assertTrue(!resourceList.isEmpty());
    // there is a success file
    Assert.assertTrue(resourceList.stream().anyMatch(f -> f.endsWith("SUCCESS")));
    // there are at least 2 files, 1 with data and the success files (this depends on when the test
    // is exec, it may generate 2 windows).
    Assert.assertTrue(resourceList.size() >= 2);
    // lets count that we are writing the expected number of rows in the parquet file
    Assert.assertTrue(
        checkNumRows(
            resourceList.stream().filter(f -> !f.endsWith("SUCCESS")).findFirst().get(), 100));
  }

  private void setupPipeline(
      WriteFormatToFileDestination<GenericRecord> writeFormat, int avroRecords) {

    String outputPath = temporaryFolder.getRoot().getAbsolutePath() + '/';
    List<GenericRecord> records = ComposeFilesTest.generateGenericRecords(avroRecords);
    AvroGenericCoder coder = AvroGenericCoder.of(ComposeFilesTest.SCHEMA);
    Instant baseTime = new Instant(0);

    TestStream.Builder<GenericRecord> streamBuilder =
        TestStream.create(coder)
            .advanceWatermarkTo(baseTime)
            .addElements(TimestampedValue.of(records.get(0), Instant.now()))
            .advanceProcessingTime(Duration.standardSeconds(1L));

    for (int i = 1; i < 99; i++) {
      streamBuilder = streamBuilder.addElements(TimestampedValue.of(records.get(i), Instant.now()));
    }

    TestStream<GenericRecord> stream =
        streamBuilder
            .addElements(TimestampedValue.of(records.get(99), Instant.now()))
            .advanceProcessingTime(Duration.standardMinutes(1L))
            .advanceWatermarkToInfinity();

    testPipeline
        .getCoderRegistry()
        .registerCoderForType(TypeDescriptor.of(GenericRecord.class), coder);

    // we will write 2 files in the temp directory
    testPipeline
        .apply(stream)
        .apply(
            Window.<GenericRecord>into(FixedWindows.of(Utilities.parseDuration("1m")))
                .discardingFiredPanes())
        .apply(
            "WriteParquetToGCS",
            writeFormat
                .withSinkProvider(
                    () ->
                        ParquetIO.sink(new Schema.Parser().parse(ComposeFilesTest.SCHEMA_STRING))
                            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED))
                .withCoder(coder)
                .withOutputDirectory(ValueProvider.StaticValueProvider.of(outputPath))
                .withOutputFilenamePrefix(ValueProvider.StaticValueProvider.of("test"))
                .withOutputFilenameSuffix(ValueProvider.StaticValueProvider.of(".parquet"))
                .withTempDirectory(ValueProvider.StaticValueProvider.of(outputPath + "temp"))
                .withSuccessFilesWindowDuration("5s")
                // to avoid generating infinit long sequences for empty windows
                .withTestingSeq());
  }

  private boolean checkNumRows(String filePath, int expectedRowCount) throws IOException {
    int rowCount = 0;
    try (ParquetReader<GenericRecord> reader =
        AvroParquetReader.<GenericRecord>builder(
                new AvroUtils.BeamParquetInputFile(Files.newByteChannel(Paths.get(filePath))))
            .build(); ) {

      GenericRecord nextRecord;
      while ((nextRecord = reader.read()) != null) {
        rowCount++;
      }
    }
    return expectedRowCount == rowCount;
  }
}
