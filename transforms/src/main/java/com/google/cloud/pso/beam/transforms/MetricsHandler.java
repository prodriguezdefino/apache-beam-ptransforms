/*
 * Copyright (C) 2021 Google Inc.
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

import com.google.api.Metric;
import com.google.api.MonitoredResource;
import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.math.Quantiles;
import com.google.monitoring.v3.CreateTimeSeriesRequest;
import com.google.monitoring.v3.Point;
import com.google.monitoring.v3.TimeInterval;
import com.google.monitoring.v3.TimeSeries;
import com.google.monitoring.v3.TypedValue;
import com.google.protobuf.util.Timestamps;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.ApproximateQuantiles;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.transforms.windowing.AfterFirst;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MetricsHandler
        extends PTransform<PCollection<List<Long>>, PDone> {

  public static enum MetricComputationType {
    DISTRIBUTION,
    PERCENTILE,
    PERCENTILE_SAMPLE,
    NONE;
  }

  private static final Logger LOG = LoggerFactory.getLogger(MetricsHandler.class);
  public static final String P50_METRIC_KEY = "p50";
  public static final String P95_METRIC_KEY = "p95";
  public static final String P99_METRIC_KEY = "p99";
  public static final String MIN_METRIC_KEY = "p1";
  public static final String MAX_METRIC_KEY = "p100";
  private static final Integer DEFAULT_MAX_PANE_SECONDS = 30;
  private static final Integer DEFAULT_COMBINE_FANOUT = 5;
  private static final Integer DEFAULT_MAX_PANE_ELEMENTS = 1000000;
  protected Integer maxPaneSeconds = DEFAULT_MAX_PANE_SECONDS;
  protected Integer combineFanout = DEFAULT_COMBINE_FANOUT;
  protected Integer maxPaneElements = DEFAULT_MAX_PANE_ELEMENTS;
  protected final String metricName;

  private MetricsHandler(String metricName) {
    this.metricName = metricName;
  }

  public static MetricsHandler create(String metricName, MetricComputationType type) {
    switch (type) {
      case DISTRIBUTION: {
        return new DistributionComputation(metricName);
      }
      case PERCENTILE: {
        return new PercentileComputation(metricName);
      }
      case PERCENTILE_SAMPLE: {
        return new PercentileSampleComputation(metricName);
      }
      case NONE: {
        return new NoOpHandler();
      }
    }
    throw new IllegalArgumentException("Unrecognized metric calculation type.");
  }

  public MetricsHandler withMaxAggregationSeconds(Integer seconds) {
    this.maxPaneSeconds = seconds;
    return this;
  }

  public MetricsHandler withMaxAggregationElements(Integer elements) {
    this.maxPaneElements = elements;
    return this;
  }

  Trigger getTrigger() {
    return AfterWatermark
            .pastEndOfWindow()
            .withEarlyFirings(
                    AfterFirst.of(
                            AfterPane
                                    .elementCountAtLeast(this.maxPaneElements),
                            AfterProcessingTime
                                    .pastFirstElementInPane()
                                    .plusDelayOf(
                                            Duration.standardSeconds(
                                                    this.maxPaneSeconds))));
  }

  static abstract class AbstractMetricsPropagation<T>
          extends DoFn<T, Void> {

    private String projectName;
    private TimeSeries.Builder timeSeriesBuilder;
    private MetricServiceClient metricServiceClient;
    protected Map<String, Metric> metrics = Maps.newHashMap();
    private final String metricName;

    public AbstractMetricsPropagation(String metricName) {
      this.metricName = metricName;
    }

    @Setup
    public void bundleSetup() throws IOException {
      this.metricServiceClient = MetricServiceClient.create();
    }

    @StartBundle
    public void startBundle(PipelineOptions options) {
      this.projectName = options.as(DataflowWorkerHarnessOptions.class).getProject();
      HashMap metricLabels = Maps.newHashMap();
      metricLabels.put("transform", "CaptureMetrics");
      metricLabels.put("job_id", options.as(DataflowWorkerHarnessOptions.class).getJobId());
      this.metrics.put(
              MetricsHandler.P50_METRIC_KEY,
              Metric.newBuilder()
                      .setType("custom.googleapis.com/dataflow/" + this.metricName + "_P50")
                      .putAllLabels(metricLabels)
                      .build());
      this.metrics.put(
              MetricsHandler.P95_METRIC_KEY,
              Metric.newBuilder()
                      .setType("custom.googleapis.com/dataflow/" + this.metricName + "_P95")
                      .putAllLabels(metricLabels)
                      .build());
      this.metrics.put(
              MetricsHandler.P99_METRIC_KEY,
              Metric.newBuilder()
                      .setType("custom.googleapis.com/dataflow/" + this.metricName + "_P99")
                      .putAllLabels(metricLabels)
                      .build());
      this.metrics.put(
              MetricsHandler.MIN_METRIC_KEY,
              Metric.newBuilder().
                      setType("custom.googleapis.com/dataflow/" + this.metricName + "_MIN")
                      .putAllLabels(metricLabels)
                      .build());
      this.metrics.put(
              MetricsHandler.MAX_METRIC_KEY,
              Metric.newBuilder()
                      .setType("custom.googleapis.com/dataflow/" + this.metricName + "_MAX")
                      .putAllLabels(metricLabels)
                      .build());
      Map<String, String> resourceLabels = Maps.newHashMap();
      resourceLabels.put("project_id", this.projectName);
      resourceLabels.put("job_name", options.as(DataflowWorkerHarnessOptions.class).getJobName());
      resourceLabels.put("region", options.as(DataflowWorkerHarnessOptions.class).getRegion());
      this.timeSeriesBuilder = TimeSeries
              .newBuilder()
              .setResource(
                      MonitoredResource
                              .newBuilder()
                              .setType("dataflow_job")
                              .putAllLabels(resourceLabels)
                              .build());
    }

    Point createPointFromValue(long timeInMillis, double value) {
      return Point.newBuilder()
              .setInterval(
                      TimeInterval.newBuilder()
                              .setEndTime(
                                      Timestamps.fromMillis(timeInMillis))
                              .build())
              .setValue(
                      TypedValue.newBuilder()
                              .setDoubleValue(value)
                              .build())
              .build();
    }

    CreateTimeSeriesRequest createRequest(Metric metric, List<Point> monitoringPoints) {
      return CreateTimeSeriesRequest.newBuilder()
              .setName("projects/" + this.projectName)
              .addAllTimeSeries(
                      monitoringPoints.stream()
                              .map(p -> this.timeSeriesBuilder
                              .clearPoints()
                              .clearMetric()
                              .setMetric(metric)
                              .addPoints(p)
                              .build())
                              .collect(Collectors.toList())).build();
    }

    void propagateMetricPoint(Metric metric, long timeInMillis, double value) {
      try {
        this.metricServiceClient.createTimeSeries(
                this.createRequest(
                        metric,
                        Lists.newArrayList(
                                this.createPointFromValue(timeInMillis, value))));
      } catch (Exception ex) {
        LOG.warn("Errors occurred while propagating metrics, values are discarded.", ex);
      }
    }

    @Teardown
    public void teardown() {
      this.metricServiceClient.close();
    }
  }

  public static class DistributionComputation
          extends MetricsHandler {

    private DistributionComputation(String metricName) {
      super(metricName);
    }

    @Override
    public PDone expand(PCollection<List<Long>> input) {
      input.apply("CaptureOnDistribution",
              ParDo.of(new CaptureMetricsOnDistribution(this.metricName)));
      return PDone.in((Pipeline) input.getPipeline());
    }

    static class CaptureMetricsOnDistribution
            extends DoFn<List<Long>, Void> {

      private static Distribution pubsubReceivedIngestionLatencyMetric = null;
      private final String metricName;

      private CaptureMetricsOnDistribution(String metricName) {
        this.metricName = metricName;
      }

      static Distribution distribution(String metricName) {
        if (pubsubReceivedIngestionLatencyMetric == null) {
          synchronized (CaptureMetricsOnDistribution.class) {
            if (pubsubReceivedIngestionLatencyMetric == null) {
              pubsubReceivedIngestionLatencyMetric
                      = Metrics.distribution(CaptureMetricsOnDistribution.class, metricName);
            }
          }
        }
        return pubsubReceivedIngestionLatencyMetric;
      }

      @ProcessElement
      public void process(ProcessContext context) {
        if (context.element().isEmpty()) {
          return;
        }
        distribution(this.metricName).update(context.element().get(0));
      }
    }
  }

  public static class PercentileSampleComputation
          extends MetricsHandler {

    private Integer sampleCount = 10;

    private PercentileSampleComputation(String metricName) {
      super(metricName);
      this.maxPaneElements = this.sampleCount * 10;
    }

    public PercentileSampleComputation withSampleCount(Integer count) {
      this.sampleCount = count;
      return this;
    }

    @Override
    public PDone expand(PCollection<List<Long>> input) {
      input
              .apply("1minWindow",
                      Window.<List<Long>>into(new GlobalWindows())
                              .triggering(this.getTrigger())
                              .withAllowedLateness(Duration.ZERO)
                              .discardingFiredPanes())
              .apply("Sample" + this.sampleCount,
                      Sample.any(this.sampleCount))
              .apply("PropagateMetrics",
                      ParDo.of(new ProcessAndPropagateSampleMetrics(this.metricName)));
      return PDone.in(input.getPipeline());
    }

    static class ProcessAndPropagateSampleMetrics
            extends AbstractMetricsPropagation<List<Long>> {

      private final List<Long> latencies = Lists.newLinkedList();
      private Long lastSentMillisFromEpoch = 0L;

      public ProcessAndPropagateSampleMetrics(String name) {
        super(name);
      }

      @ProcessElement
      public void process(ProcessContext context) {
        this.latencies.addAll(context.element());
      }

      void propagateMetricValues(Long currentTimestamp) {
        Map<Integer, Double> perc
                = Quantiles
                        .percentiles()
                        .indexes(new int[]{1, 50, 95, 99, 100})
                        .compute(this.latencies);
        this.propagateMetricPoint(
                this.metrics.get(MetricsHandler.P50_METRIC_KEY), currentTimestamp, perc.get(50));
        this.propagateMetricPoint(
                this.metrics.get(MetricsHandler.MIN_METRIC_KEY), currentTimestamp, perc.get(1));
        this.propagateMetricPoint(
                this.metrics.get(MetricsHandler.P95_METRIC_KEY), currentTimestamp, perc.get(95));
        this.propagateMetricPoint(
                this.metrics.get(MetricsHandler.P99_METRIC_KEY), currentTimestamp, perc.get(99));
        this.propagateMetricPoint(
                this.metrics.get(MetricsHandler.MAX_METRIC_KEY), currentTimestamp, perc.get(100));
        this.latencies.clear();
        this.lastSentMillisFromEpoch = currentTimestamp;
      }

      @DoFn.FinishBundle
      public void finishBundle() {
        Long now = Instant.now().getMillis();
        if (now - this.lastSentMillisFromEpoch > 60000L) {
          this.propagateMetricValues(now);
        }
      }

      @Override
      public void teardown() {
        if (!this.latencies.isEmpty()) {
          this.propagateMetricValues(Instant.now().getMillis());
        }
        super.teardown();
      }
    }
  }

  public static class PercentileComputation
          extends MetricsHandler {

    private PercentileComputation(String metricName) {
      super(metricName);
    }

    public PercentileComputation withFanout(Integer fanout) {
      this.combineFanout = fanout;
      return this;
    }

    @Override
    public PDone expand(PCollection<List<Long>> input) {
      input
              .apply("MapToLong", Flatten.iterables())
              .apply(this.maxPaneSeconds + "secPanes",
                      Window.<Long>into(new GlobalWindows())
                              .triggering(this.getTrigger())
                              .withAllowedLateness(Duration.ZERO)
                              .discardingFiredPanes())
              .apply("CalculatePercentileOnWindow",
                      Combine.globally(
                              ApproximateQuantiles.ApproximateQuantilesCombineFn
                                      .<Long>create(101))
                              .withFanout(this.combineFanout))
              .apply("PropagateMetrics",
                      ParDo.of(new PropagateDistributionMetrics(this.metricName)));
      return PDone.in((Pipeline) input.getPipeline());
    }

    static class PropagateDistributionMetrics
            extends AbstractMetricsPropagation<List<Long>> {

      public PropagateDistributionMetrics(String name) {
        super(name);
      }

      @ProcessElement
      public void process(ProcessContext context) {
        if (((List) context.element()).size() < 101) {
          return;
        }
        long currentTime = Instant.now().getMillis();
        this.propagateMetricPoint(
                this.metrics.get(MetricsHandler.P50_METRIC_KEY), currentTime, context.element().get(50));
        this.propagateMetricPoint(
                this.metrics.get(MetricsHandler.P95_METRIC_KEY), currentTime, context.element().get(95));
        this.propagateMetricPoint(
                this.metrics.get(MetricsHandler.P99_METRIC_KEY), currentTime, context.element().get(99));
        this.propagateMetricPoint(
                this.metrics.get(MetricsHandler.MIN_METRIC_KEY), currentTime, context.element().get(0));
        this.propagateMetricPoint(
                this.metrics.get(MetricsHandler.MAX_METRIC_KEY), currentTime, context.element().get(100));
      }
    }
  }

  public static class NoOpHandler
          extends MetricsHandler {

    private NoOpHandler() {
      super("noop");
    }

    @Override
    public PDone expand(PCollection<List<Long>> input) {
      input.apply("NOOP", (PTransform) ParDo.of((DoFn) new DoFn<List<Long>, Void>() {

        @DoFn.ProcessElement
        public void process(DoFn.ProcessContext context) {
        }
      }));
      return PDone.in((Pipeline) input.getPipeline());
    }
  }

}
