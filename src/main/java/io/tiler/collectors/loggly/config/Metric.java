package io.tiler.collectors.loggly.config;

import io.tiler.core.time.TimePeriodParser;

import java.util.List;

public class Metric {
  private final String name;
  private final long timePeriodInMicroseconds;
  private final int retryTimes;
  private final String query;
  private final List<Field> fields;

  public Metric(String name, String timePeriod, Integer retryTimes, String query, List<Field> fields) {
    if (timePeriod == null) {
      timePeriod = "1d";
    }

    if (retryTimes == null) {
      retryTimes = 3;
    }

    this.name = name;
    this.timePeriodInMicroseconds = TimePeriodParser.parseTimePeriodToMicroseconds(timePeriod);
    this.retryTimes = retryTimes;
    this.query = query;
    this.fields = fields;
  }

  public String name() {
    return name;
  }

  public long timePeriodInMicroseconds() {
    return timePeriodInMicroseconds;
  }

  public int retryTimes() {
    return retryTimes;
  }

  public String query() {
    return query;
  }

  public boolean hasQuery() {
    return query != null;
  }

  public List<Field> fields() {
    return fields;
  }
}
