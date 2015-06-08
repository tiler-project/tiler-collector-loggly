package io.tiler.collectors.loggly.config;

import io.tiler.core.time.TimePeriodParser;

import java.util.List;

public class Config {
  private final long collectionIntervalInMilliseconds;
  private final List<Server> servers;
  private final String metricNamePrefix;

  public Config(String collectionInterval, List<Server> servers, String metricNamePrefix) {
    if (collectionInterval == null) {
      collectionInterval = "1h";
    }

    if (metricNamePrefix == null) {
      metricNamePrefix = "loggly.";
    }

    this.collectionIntervalInMilliseconds = TimePeriodParser.parseTimePeriodToMilliseconds(collectionInterval);
    this.servers = servers;
    this.metricNamePrefix = metricNamePrefix;
  }

  public long collectionIntervalInMilliseconds() {
    return collectionIntervalInMilliseconds;
  }

  public List<Server> servers() {
    return servers;
  }

  public String metricNamePrefix() {
    return metricNamePrefix;
  }

  public String getFullMetricName(Metric metric) {
    return metricNamePrefix() + metric.name();
  }
}
