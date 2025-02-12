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
package com.google.cloud.pso.beam.common;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.MutablePeriod;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.joda.time.format.PeriodFormatterBuilder;

/** A collection of static methods for Date manipulation. */
public class Utilities {

  private static final String OUTPUT_PATH_MINUTE_WINDOW = "year=YYYY/month=MM/day=DD/hour=HH/minute=mm/";
  private static final String OUTPUT_PATH_HOURLY_WINDOW = "year=YYYY/month=MM/day=DD/hour=HH/";
  private static final String OUTPUT_PATH_FLAT_WINDOW_MINUTE = "YYYYMMddHHmm";
  private static final String OUTPUT_PATH_FLAT_WINDOW_HOUR = "YYYYMMddHH";
  private static final DateTimeFormatter OUTPUT_HOURLY_WINDOW_FILENAME_COMPONENT =
      ISODateTimeFormat.basicDateTime();
  private static final DateTimeFormatter YEAR = DateTimeFormat.forPattern("YYYY");
  private static final DateTimeFormatter MONTH = DateTimeFormat.forPattern("MM");
  private static final DateTimeFormatter DAY = DateTimeFormat.forPattern("dd");
  private static final DateTimeFormatter HOUR = DateTimeFormat.forPattern("HH");
  private static final DateTimeFormatter MINUTE = DateTimeFormat.forPattern("mm");
  private static final DateTimeFormatter MINUTE_GRANULARITY_TS =
      DateTimeFormat.forPattern(OUTPUT_PATH_FLAT_WINDOW_MINUTE);
  private static final DateTimeFormatter HOUR_GRANULARITY_TS =
      DateTimeFormat.forPattern(OUTPUT_PATH_FLAT_WINDOW_HOUR);
  private static final Random RANDOM = new Random(System.currentTimeMillis());

  /**
   * @param time
   * @return
   */
  public static String buildPartitionedPathFromDatetime(DateTime time) {
    return OUTPUT_PATH_MINUTE_WINDOW
        .replace("YYYY", YEAR.print(time))
        .replace("MM", MONTH.print(time))
        .replace("DD", DAY.print(time))
        .replace("HH", HOUR.print(time))
        .replace("mm", MINUTE.print(time));
  }

  /**
   * @param time
   * @return
   */
  public static String buildHourlyPartitionedPathFromDatetime(DateTime time) {
    return OUTPUT_PATH_HOURLY_WINDOW
        .replace("YYYY", YEAR.print(time))
        .replace("MM", MONTH.print(time))
        .replace("DD", DAY.print(time))
        .replace("HH", HOUR.print(time));
  }

  /**
   * Formats the provided time to the format expected for the window component of the filename.
   *
   * @param time
   * @return
   */
  public static String formatFilenameWindowComponent(DateTime time) {
    return time.toString(OUTPUT_HOURLY_WINDOW_FILENAME_COMPONENT);
  }

  /**
   * Parses a duration from a period formatted string. Values are accepted in the following formats:
   *
   * <p>Formats Ns - Seconds. Example: 5s<br>
   * Nm - Minutes. Example: 13m<br>
   * Nh - Hours. Example: 2h
   *
   * <pre>
   * parseDuration(null) = NullPointerException()
   * parseDuration("")   = Duration.standardSeconds(0)
   * parseDuration("2s") = Duration.standardSeconds(2)
   * parseDuration("5m") = Duration.standardMinutes(5)
   * parseDuration("3h") = Duration.standardHours(3)
   * </pre>
   *
   * @param value The period value to parse.
   * @return The {@link Duration} parsed from the supplied period string.
   */
  public static Duration parseDuration(String value) {
    checkNotNull(value, "The specified duration as string must be a non-null value!");

    var parser =
        new PeriodFormatterBuilder()
            .appendSeconds()
            .appendSuffix("s")
            .appendMinutes()
            .appendSuffix("m")
            .appendHours()
            .appendSuffix("h")
            .toParser();

    var period = new MutablePeriod();
    parser.parseInto(period, value, 0, Locale.getDefault());

    var duration = period.toDurationFrom(new DateTime(0));
    checkArgument(duration.getMillis() > 0, "The window duration must be greater than 0!");

    return duration;
  }

  public static String buildFlatPathFromDateTime(DateTime time) {
    return OUTPUT_PATH_FLAT_WINDOW_MINUTE
        .replace("YYYY", YEAR.print(time))
        .replace("MM", MONTH.print(time))
        .replace("DD", DAY.print(time))
        .replace("HH", HOUR.print(time))
        .replace("mm", MINUTE.print(time));
  }

  public static String formatMinuteGranularityTimestamp(Instant instant) {
    return MINUTE_GRANULARITY_TS.print(instant);
  }

  public static String formatHourGranularityTimestamp(Instant instant) {
    return HOUR_GRANULARITY_TS.print(instant);
  }

  public static TableSchema addNullableTimestampColumnToBQSchema(
      TableSchema bqSchema, String fieldName) {
    var fields = new ArrayList<>(bqSchema.getFields());
    fields.add(new TableFieldSchema().setName(fieldName).setType("TIMESTAMP").setMode("NULLABLE"));
    return new TableSchema().setFields(fields);
  }

  public static Schema addNullableTimestampFieldToAvroSchema(Schema base, String fieldName) {
    var timestampMilliType =
        Schema.createUnion(
            Lists.newArrayList(
                Schema.create(Schema.Type.NULL),
                LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))));

    var baseFields =
        base.getFields().stream()
            .map(
                field ->
                    new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal()))
            .collect(Collectors.toList());

    baseFields.add(
        new Schema.Field(fieldName, timestampMilliType, null, JsonProperties.NULL_VALUE));

    return Schema.createRecord(
        base.getName(), base.getDoc(), base.getNamespace(), false, baseFields);
  }

  /**
   * Returns an integer in the provided range with a high probability to be skewed given the params
   * provided.
   *
   * <p>Based on https://stackoverflow.com/a/13548135
   *
   * @param min the minimum skewed value possible
   * @param max the maximum skewed value possible
   * @param skew the degree to which the values cluster around the mode of the distribution; higher
   *     values mean tighter clustering
   * @param bias the tendency of the mode to approach the min, max or midpoint value; positive
   *     values bias toward max, negative values toward min
   * @return An integer in the provided range with a high probability to be skewed given the params
   *     provided.
   */
  public static Integer nextSkewedBoundedInteger(
      Integer min, Integer max, double skew, double bias) {
    var range = max - min;
    var mid = min + range / 2.0;
    var unitGaussian = RANDOM.nextGaussian();
    var biasFactor = Math.exp(bias);
    Double retval =
        mid + (range * (biasFactor / (biasFactor + Math.exp(-unitGaussian / skew)) - 0.5));
    return retval.intValue();
  }
}
