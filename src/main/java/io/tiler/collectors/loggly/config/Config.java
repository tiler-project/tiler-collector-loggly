package io.tiler.collectors.loggly.config;

import java.util.List;

public class Config {
  private final long collectionIntervalInMilliseconds;
  private final List<Server> servers;
  private final String metricNamePrefix;

  public Config(long collectionIntervalInMilliseconds, List<Server> servers, String metricNamePrefix) {
    this.collectionIntervalInMilliseconds = collectionIntervalInMilliseconds;
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
}
