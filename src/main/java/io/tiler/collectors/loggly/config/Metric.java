package io.tiler.collectors.loggly.config;

import io.tiler.core.time.TimePeriodParser;

import java.util.List;

public class Metric {
  private final String name;
  private final long intervalInMicroseconds;
  private final long retentionPeriodInMicroseconds;
  private final long maxCatchUpPeriodInMicroseconds;
  private final long stabilityPeriodInMilliseconds;
  private final int retryTimes;
  private final List<Field> fields;

  public Metric(String name, String interval, String retentionPeriod, String maxCatchUpPeriod, String stabilityPeriod, Integer retryTimes, List<Field> fields) {
    if (interval == null) {
      interval = "1h";
    }

    if (retentionPeriod == null) {
      retentionPeriod = "1d";
    }

    if (maxCatchUpPeriod == null) {
      maxCatchUpPeriod = "1d";
    }

    if (stabilityPeriod == null) {
      stabilityPeriod = "1h";
    }

    if (retryTimes == null) {
      retryTimes = 3;
    }

    this.name = name;
    this.intervalInMicroseconds = TimePeriodParser.parseTimePeriodToMicroseconds(interval);
    this.retentionPeriodInMicroseconds = TimePeriodParser.parseTimePeriodToMicroseconds(retentionPeriod);
    this.maxCatchUpPeriodInMicroseconds = TimePeriodParser.parseTimePeriodToMicroseconds(maxCatchUpPeriod);
    this.stabilityPeriodInMilliseconds = TimePeriodParser.parseTimePeriodToMicroseconds(stabilityPeriod);
    this.retryTimes = retryTimes;
    this.fields = fields;
  }

  public String name() {
    return name;
  }

  public long intervalInMicroseconds() {
    return intervalInMicroseconds;
  }

  public long retentionPeriodInMicroseconds() {
    return retentionPeriodInMicroseconds;
  }

  public long maxCatchUpPeriodInMicroseconds() {
    return maxCatchUpPeriodInMicroseconds;
  }

  public long stabilityPeriodInMilliseconds() {
    return stabilityPeriodInMilliseconds;
  }

  public int retryTimes() {
    return retryTimes;
  }

  public List<Field> fields() {
    return fields;
  }
}
