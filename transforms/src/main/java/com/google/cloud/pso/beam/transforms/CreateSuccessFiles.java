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

import static com.google.cloud.pso.beam.common.Utilities.buildFlatPathFromDateTime;
import static com.google.cloud.pso.beam.common.Utilities.buildHourlyPartitionedPathFromDatetime;
import static com.google.cloud.pso.beam.common.Utilities.buildPartitionedPathFromDatetime;
import static com.google.cloud.pso.beam.common.Utilities.parseDuration;
import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Hours;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates SUCCESS files based on the data contained in 2 tuples of the PCollectionTuple: data that
 * has been processed on a particular window (processedData) and signals of data found in the same
 * window (dataOnWindowSignals).
 */
public class CreateSuccessFiles extends PTransform<PCollectionTuple, PDone> {

  private static final Logger LOG = LoggerFactory.getLogger(CreateSuccessFiles.class);

  private TupleTag<Boolean> dataOnWindowSignals;
  private TupleTag<String> processedData;
  private Integer fanoutShards = 10;
  private String windowDuration;
  private ValueProvider<String> outputDirectory;
  private Boolean testingSeq = false;
  private String successFileNamePrefix = "_SUCCESS";
  private Boolean flatNamingStructure = false;

  public CreateSuccessFiles withProcessedDataTag(TupleTag<String> processedData) {
    this.processedData = processedData;
    return this;
  }

  @VisibleForTesting
  CreateSuccessFiles withTestingSeq() {
    this.testingSeq = true;
    return this;
  }

  public CreateSuccessFiles withDataOnWindowSignalsTag(TupleTag<Boolean> dataOnWindowSignals) {
    this.dataOnWindowSignals = dataOnWindowSignals;
    return this;
  }

  public CreateSuccessFiles withOutputDirectory(ValueProvider<String> outputDirectory) {
    this.outputDirectory = outputDirectory;
    return this;
  }

  public CreateSuccessFiles withSuccessFileWindowDuration(String windowDuration) {
    this.windowDuration = windowDuration;
    return this;
  }

  public CreateSuccessFiles withFanoutShards(Integer fanoutShards) {
    this.fanoutShards = fanoutShards;
    return this;
  }

  public CreateSuccessFiles withSuccessFilePrefix(String prefix) {
    this.successFileNamePrefix = prefix;
    return this;
  }

  public CreateSuccessFiles withFlatNamingStructure(Boolean flatStructure) {
    this.flatNamingStructure = flatStructure;
    return this;
  }

  public static CreateSuccessFiles create() {
    return new CreateSuccessFiles();
  }

  public static <String> TupleTag<String> processedDataTag() {
    return new TupleTag<String>() {};
  }

  public static TupleTag<Boolean> dataOnWindowSignalTag() {
    return new TupleTag<Boolean>() {};
  }

  @Override
  public void validate(PipelineOptions options) {
    super.validate(options);

    checkArgument(
        windowDuration != null,
        "A window duration should be provided using withWindowDuration method");
    checkArgument(
        outputDirectory != null, "An output directory should be provided using with method");
    checkArgument(
        dataOnWindowSignals != null && processedData != null,
        "Proper TupleTags must be configured for this transform unsing with*Tag method.");
  }

  @Override
  public PDone expand(PCollectionTuple input) {
    // check if the expected tags are included in the PCollectionTuple
    if (!input.has(dataOnWindowSignals) || !input.has(processedData)) {
      throw new IllegalArgumentException(
          "Writes to GCS expects 2 tuple tags on PCollection (data to ingest and signals on windows).");
    }

    var writeOnEmpty =
        WriteSuccessFileOnEmptyWindow.create()
            .withOutputDirectory(outputDirectory)
            .withFanoutShards(fanoutShards)
            .withWindowDuration(windowDuration)
            .withFlatNamingStructure(flatNamingStructure)
            .withSuccessFilePrefix(successFileNamePrefix);

    if (testingSeq) {
      writeOnEmpty = writeOnEmpty.withTestingSeq();
    }

    // Process an empty window, in case no data is coming from pubsub
    input
        .get(dataOnWindowSignals)
        // create a SUCCESS file if the window is empty
        .apply("ProcessEmptyWindows", writeOnEmpty);

    // also, process the PCollection with info of files that were writen to destination
    input
        .get(processedData)
        .apply(
            "WriteSuccessFile",
            CreateSuccessFileOnPresentData.create()
                .withFanoutShards(fanoutShards)
                .withWindowDuration(windowDuration)
                .withFlatNamingStructure(flatNamingStructure)
                .withSuccessFilePrefix(successFileNamePrefix));

    return PDone.in(input.getPipeline());
  }

  /**
   * Given a String PCollection with the file names contained in a window, will wait for all of them
   * to be completed and create a SUCCESS file in the containing directory (All files are expected
   * to be contained in the same directory).
   */
  static class CreateSuccessFileOnPresentData
      extends PTransform<PCollection<String>, PCollection<Void>> {

    private Integer fanoutShards = 10;
    private String windowDuration;
    private String successFileNamePrefix = "_SUCCESS";
    private Boolean flatNamingStructure = false;

    public CreateSuccessFileOnPresentData() {}

    public static CreateSuccessFileOnPresentData create() {
      return new CreateSuccessFileOnPresentData();
    }

    public CreateSuccessFileOnPresentData withFanoutShards(Integer fanoutShards) {
      this.fanoutShards = fanoutShards;
      return this;
    }

    public CreateSuccessFileOnPresentData withWindowDuration(String windowDuration) {
      this.windowDuration = windowDuration;
      return this;
    }

    public CreateSuccessFileOnPresentData withSuccessFilePrefix(String prefix) {
      this.successFileNamePrefix = prefix;
      return this;
    }

    public CreateSuccessFileOnPresentData withFlatNamingStructure(Boolean flatStructure) {
      this.flatNamingStructure = flatStructure;
      return this;
    }

    @Override
    public PCollection<Void> expand(PCollection<String> input) {
      return input
          // wait for all the files in the current window
          .apply(
              "With" + windowDuration + "Window",
              Window.<String>into(FixedWindows.of(parseDuration(windowDuration)))
                  .withAllowedLateness(parseDuration(windowDuration).dividedBy(4L))
                  .discardingFiredPanes())
          .apply(
              "CombineFilesInWindow",
              Combine.globally(CombineFilesNames.create())
                  .withFanout(fanoutShards)
                  .withoutDefaults())
          .apply(
              "CreateSuccessFile",
              ParDo.of(
                  new SuccessFileWriteDoFn()
                      .withFlatNamingStructure(flatNamingStructure)
                      .withSuccessFilePrefix(successFileNamePrefix)));
    }

    /**
     * Combine Strings keeping the latest filename (ordered lexicographically) as the result to be
     * returned.
     */
    static class CombineFilesNames
        extends Combine.CombineFn<String, CombineFilesNames.FilenameAcc, String> {

      static class FilenameAcc implements Serializable {

        private String filename;

        public void add(String anotherFileName) {
          if (anotherFileName == null) {
            return;
          } else if (filename == null) {
            filename = anotherFileName;
          } else if (filename.compareTo(anotherFileName) > 0) {
            filename = anotherFileName;
          }
        }

        public void merge(FilenameAcc accu) {
          this.add(accu.filename);
        }

        @Override
        public int hashCode() {
          var hash = 7;
          hash = 41 * hash + Objects.hashCode(this.filename);
          return hash;
        }

        @Override
        public boolean equals(Object obj) {
          if (this == obj) {
            return true;
          }
          if (obj == null) {
            return false;
          }
          if (getClass() != obj.getClass()) {
            return false;
          }
          final var other = (FilenameAcc) obj;
          return Objects.equals(this.filename, other.filename);
        }
      }

      public static CombineFilesNames create() {
        return new CombineFilesNames();
      }

      @Override
      public CombineFilesNames.FilenameAcc createAccumulator() {
        return new FilenameAcc();
      }

      @Override
      public CombineFilesNames.FilenameAcc addInput(
          CombineFilesNames.FilenameAcc mutableAccumulator, String input) {
        mutableAccumulator.add(input);
        return mutableAccumulator;
      }

      @Override
      public CombineFilesNames.FilenameAcc mergeAccumulators(
          Iterable<CombineFilesNames.FilenameAcc> accumulators) {
        var newAccum = createAccumulator();
        for (var accum : accumulators) {
          newAccum.merge(accum);
        }
        return newAccum;
      }

      @Override
      public String extractOutput(CombineFilesNames.FilenameAcc accumulator) {
        // return a consistent representation of a file list to avoid duplications when retries
        // happens
        return accumulator.filename;
      }
    }

    /**
     * Creates a SUCCESS file on the folder location of the first file in the received iterable
     * (assumes all the files are contained in the same folder).
     */
    static class SuccessFileWriteDoFn extends DoFn<String, Void> {

      private String successFileNamePrefix = "_SUCCESS";
      private Boolean flatNamingStructure = false;

      public SuccessFileWriteDoFn withSuccessFilePrefix(String prefix) {
        this.successFileNamePrefix = prefix;
        return this;
      }

      public SuccessFileWriteDoFn withFlatNamingStructure(Boolean flatStructure) {
        this.flatNamingStructure = flatStructure;
        return this;
      }

      @ProcessElement
      public void processElement(ProcessContext context, BoundedWindow window) throws IOException {
        var fileName = successFileNamePrefix;

        if (flatNamingStructure && window instanceof IntervalWindow) {
          var intervalWindow = (IntervalWindow) window;
          fileName =
              fileName
                  + "_"
                  + buildFlatPathFromDateTime(intervalWindow.start().toDateTime())
                  + "_"
                  + buildFlatPathFromDateTime(intervalWindow.end().toDateTime());
        }

        createSuccessFileInPath(context.element(), fileName, false);
        context.output((Void) null);
      }
    }
  }

  /**
   * In charge of inspecting each window the pipeline triggers and count the events occurring on it,
   * since the pipeline contains a side input that periodically generates dummy signals, if in any
   * window only one signal is present the pipeline has not received any data from its main source.
   */
  static class WriteSuccessFileOnEmptyWindow extends PTransform<PCollection<Boolean>, PDone> {

    private String windowDuration;
    private Integer fanoutShards = 10;
    private Boolean testingSeq = false;
    private ValueProvider<String> outputDirectory;
    private String successFileNamePrefix = "_SUCCESS";
    private Boolean flatNamingStructure = false;

    private WriteSuccessFileOnEmptyWindow() {}

    public static WriteSuccessFileOnEmptyWindow create() {
      return new WriteSuccessFileOnEmptyWindow();
    }

    public WriteSuccessFileOnEmptyWindow withFanoutShards(Integer fanoutShards) {
      this.fanoutShards = fanoutShards;
      return this;
    }

    public WriteSuccessFileOnEmptyWindow withWindowDuration(String winDurationStr) {
      this.windowDuration = winDurationStr;
      return this;
    }

    public WriteSuccessFileOnEmptyWindow withOutputDirectory(ValueProvider<String> outputDir) {
      this.outputDirectory = outputDir;
      return this;
    }

    public WriteSuccessFileOnEmptyWindow withSuccessFilePrefix(String prefix) {
      this.successFileNamePrefix = prefix;
      return this;
    }

    public WriteSuccessFileOnEmptyWindow withFlatNamingStructure(Boolean flatStructure) {
      this.flatNamingStructure = flatStructure;
      return this;
    }

    @VisibleForTesting
    public WriteSuccessFileOnEmptyWindow withTestingSeq() {
      this.testingSeq = true;
      return this;
    }

    @Override
    public void validate(PipelineOptions options) {
      super.validate(options);

      checkArgument(
          windowDuration != null,
          "A window duration should be provided using the withWindowDuration method.");
      checkArgument(
          outputDirectory != null,
          "An output directory should be provided using the withOutputDirectory method.");
    }

    @Override
    @SuppressWarnings("deprecation")
    public PDone expand(PCollection<Boolean> input) {
      var window =
          Window.<Boolean>into(FixedWindows.of(parseDuration(windowDuration)))
              .withAllowedLateness(parseDuration(windowDuration).dividedBy(4L))
              .discardingFiredPanes();

      GenerateSequence seq = GenerateSequence.from(0l).withRate(1, parseDuration(windowDuration));

      // when testing we only want one impulse to be generated.
      if (testingSeq) {
        seq = seq.to(1L);
      }

      // create a dummy signal on periodic intervals using same window definition
      var periodicSignals =
          input
              .getPipeline()
              .apply("ImpulseEvery" + windowDuration, seq)
              .apply(
                  "CreateDummySignal", MapElements.into(TypeDescriptors.booleans()).via(ts -> true))
              .apply(windowDuration + "Window", window);

      // flatten elements with the input branch (main data)
      PCollectionList.of(periodicSignals)
          .and(input.apply("Window" + windowDuration, window))
          .apply("FlattenSignals", Flatten.pCollections())
          .apply(
              "CountOnWindow",
              Combine.globally(Count.<Boolean>combineFn())
                  .withFanout(fanoutShards)
                  .withoutDefaults())
          .apply(
              "CheckDummySignal",
              ParDo.of(
                  new CheckDataSignalOnWindowDoFn(outputDirectory)
                      .withSuccessFilePrefix(successFileNamePrefix)
                      .withFlatNamingStructure(flatNamingStructure)));
      return PDone.in(input.getPipeline());
    }

    /** Converts an incoming {@link PubsubMessage} to the GenericRecord class */
    static class CheckDataSignalOnWindowDoFn extends DoFn<Long, Void> {

      private static final Logger LOG = LoggerFactory.getLogger(CheckDataSignalOnWindowDoFn.class);
      private static final long DUMMY_SIGNAL_ONLY_COUNT = 1L;

      private final ValueProvider<String> rootFileLocation;
      private String successFileNamePrefix = "_SUCCESS";
      private Boolean flatNamingStructure = false;

      public CheckDataSignalOnWindowDoFn(ValueProvider<String> rootFileLocation) {
        this.rootFileLocation = rootFileLocation;
      }

      public CheckDataSignalOnWindowDoFn withSuccessFilePrefix(String prefix) {
        this.successFileNamePrefix = prefix;
        return this;
      }

      public CheckDataSignalOnWindowDoFn withFlatNamingStructure(Boolean flatStructure) {
        this.flatNamingStructure = flatStructure;
        return this;
      }

      @ProcessElement
      public void processElement(ProcessContext context, BoundedWindow window, PaneInfo pane) {
        LOG.debug(
            "Found {} signals on Pane {} and Window {}.",
            context.element(),
            window.toString(),
            pane.toString());

        // if only the dummy signal has arrived in this window
        if (context.element() == DUMMY_SIGNAL_ONLY_COUNT) {
          var outputPath = rootFileLocation.isAccessible() ? rootFileLocation.get() : "";
          var fileName = successFileNamePrefix;

          if (window instanceof IntervalWindow) {
            var intervalWindow = (IntervalWindow) window;
            var time = intervalWindow.maxTimestamp().toDateTime();
            // check for hourly windows
            if (Hours.hoursBetween(intervalWindow.start(), intervalWindow.end()).getHours() == 1) {
              outputPath = outputPath + buildHourlyPartitionedPathFromDatetime(time);
            } else {
              outputPath = outputPath + buildPartitionedPathFromDatetime(time);
            }
            if (flatNamingStructure) {
              fileName =
                  fileName
                      + "_"
                      + buildFlatPathFromDateTime(intervalWindow.start().toDateTime())
                      + "_"
                      + buildFlatPathFromDateTime(intervalWindow.maxTimestamp().toDateTime());
            }
          } else {
            outputPath = outputPath + buildPartitionedPathFromDatetime(Instant.now().toDateTime());
          }

          LOG.debug("Will create SUCCESS file at {}", outputPath);
          createSuccessFileInPath(outputPath, fileName, true);
        }
      }
    }
  }

  private static void createSuccessFileInPath(String path, String fileName, boolean isDirectory) {
    // remove trailing / if exists since is not supported at the FileSystems level
    path = path.endsWith("/") ? path.substring(0, path.length() - 1) : path;

    ResourceId dirResourceFiles =
        FileSystems.matchNewResource(path, isDirectory).getCurrentDirectory();
    ResourceId successFile =
        dirResourceFiles.resolve(fileName, ResolveOptions.StandardResolveOptions.RESOLVE_FILE);

    LOG.debug("Will create success file in path {}.", successFile.toString());
    try (var writeChannel = FileSystems.create(successFile, MimeTypes.TEXT)) {
      writeChannel.write(ByteBuffer.wrap(" ".getBytes()));
    } catch (IOException ex) {
      LOG.error("Success file creation failed.", ex);
    }
  }
}
