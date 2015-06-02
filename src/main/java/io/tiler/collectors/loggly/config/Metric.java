package io.tiler.collectors.loggly.config;

import io.tiler.core.time.TimePeriodParser;

import java.util.List;

public class Metric {
  private final String name;
  private final long intervalInMicroseconds;
  private final long retentionPeriodInMicroseconds;
  private final long maxCatchUpPeriodInMicroseconds;
  private long stabilityPeriodInMilliseconds;
  private final List<Field> fields;

  public Metric(String name, String interval, String retentionPeriod, String maxCatchUpPeriod, String stabilityPeriod, List<Field> fields) {
    this.name = name;
    this.intervalInMicroseconds = TimePeriodParser.parseTimePeriodToMicroseconds(interval);
    this.retentionPeriodInMicroseconds = TimePeriodParser.parseTimePeriodToMicroseconds(retentionPeriod);
    this.maxCatchUpPeriodInMicroseconds = TimePeriodParser.parseTimePeriodToMicroseconds(maxCatchUpPeriod);
    this.stabilityPeriodInMilliseconds = TimePeriodParser.parseTimePeriodToMicroseconds(stabilityPeriod);
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

  public List<Field> fields() {
    return fields;
  }
}
