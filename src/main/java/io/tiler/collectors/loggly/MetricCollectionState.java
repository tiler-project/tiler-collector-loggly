package io.tiler.collectors.loggly;

import io.tiler.collectors.loggly.config.Config;
import io.tiler.collectors.loggly.config.Field;
import io.tiler.collectors.loggly.config.Metric;
import io.tiler.collectors.loggly.config.Server;
import io.tiler.core.json.JsonArrayIterable;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;

import java.util.Iterator;
import java.util.List;

public class MetricCollectionState {
  private final Vertx vertx;
  private final Logger logger;
  private final Config config;
  private final long currentTimeInMicroseconds;
  private boolean initialised = false;
  private int totalFieldCount;
  private final List<Server> serverConfigs;
  private List<Metric> metricConfigs;
  private List<Field> fieldConfigs;
  private Server serverConfig;
  private Metric metricConfig;
  private Field fieldConfig;
  private JsonArray servers;
  private JsonArray metrics;
  private JsonObject metric;
  private JsonArray currentPoints;
  private JsonArray nextPoints;
  private JsonObject point;
  private int serverIndex;
  private int metricIndex;
  private int fieldIndex;
  private int pointIndex;
  private long fromTimeInMicroseconds;
  private HttpClient httpClient;

  public MetricCollectionState(Vertx vertx, Logger logger, Config config, long currentTimeInMicroseconds) {
    this.vertx = vertx;
    this.logger = logger;
    this.config = config;
    this.currentTimeInMicroseconds = currentTimeInMicroseconds;
    totalFieldCount = 0;
    serverConfigs = config.servers();
    servers = new JsonArray();
  }

  public int totalFieldCount() {
    return totalFieldCount;
  }

  public Server serverConfig() {
    return serverConfig;
  }

  public Metric metricConfig() {
    return metricConfig;
  }

  public Field fieldConfig() {
    return fieldConfig;
  }

  public JsonArray servers() {
    return servers;
  }

  public int serverIndex() {
    return serverIndex;
  }

  public JsonObject metric() {
    return metric;
  }

  public JsonObject point() {
    return point;
  }

  public boolean nextPoint() {
    if (!initialised) {
      if (!nextField()) {
        return false;
      }
    }
    else {
      pointIndex++;

      if (noMorePoints()) {
        endOfPointVisit();
      }
    }

    while (noMorePoints()) {
      if (!nextField()) {
        return false;
      }
    }

    startOfPointVisit();
    return true;
  }

  private boolean noMorePoints() {
    return pointIndex >= currentPoints.size();
  }

  private void startOfPointVisit() {
    point = currentPoints.get(pointIndex);
    totalFieldCount += 1;
  }

  private void endOfPointVisit() {
  }

  private boolean nextField() {
    if (!initialised) {
      if (!nextMetric()) {
        return false;
      }
    }
    else {
      endOfFieldVisit();
      fieldIndex++;
    }

    while (noMoreFields()) {
      if (!nextMetric()) {
        return false;
      }
    }

    fieldConfig = fieldConfigs.get(fieldIndex);
    startOfFieldVisit();
    return true;
  }

  private boolean noMoreFields() {
    return fieldIndex >= fieldConfigs.size();
  }

  private void startOfFieldVisit() {
    currentPoints = nextPoints;
    nextPoints = new JsonArray();
    metric.putArray("points", nextPoints);
    pointIndex = 0;
  }

  private void endOfFieldVisit() {
  }

  private boolean nextMetric() {
    if (!initialised) {
      if (!nextServer()) {
        return false;
      }
    }
    else {
      endOfMetricVisit();
      metricIndex++;
    }

    while (noMoreMetrics()) {
      if (!nextServer()) {
        return false;
      }
    }

    metricConfig = metricConfigs.get(metricIndex);
    fieldConfigs = metricConfig.fields();
    startOfMetricVisit();
    return true;
  }

  private boolean noMoreMetrics() {
    return metricIndex >= metricConfigs.size();
  }

  private void startOfMetricVisit() {
    String metricName = config.getFullMetricName(metricConfig);

    fromTimeInMicroseconds = currentTimeInMicroseconds - metricConfig.timePeriodInMicroseconds();
    metric = new JsonObject()
      .putString("name", metricName)
      .putNumber("fromTime", fromTimeInMicroseconds)
      .putArray("points", new JsonArray());

    metrics.add(metric);

    nextPoints = new JsonArray();
    JsonObject emptyPoint = new JsonObject();
    nextPoints.addObject(emptyPoint);
    fieldIndex = 0;
  }

  private void endOfMetricVisit() {
    logger.info("Metric has " + metric.getArray("points").size() + " points");
  }

  private boolean nextServer() {
    if (!initialised) {
      serverIndex = 0;
      initialised = true;
    }
    else {
      endOfServerVisit();

      serverIndex++;
    }

    if (noMoreServers()) {
      return false;
    }

    serverConfig = serverConfigs.get(serverIndex);
    metricConfigs = serverConfig.metrics();
    startOfServerVisit();
    return true;
  }

  private boolean noMoreServers() {
    return serverIndex >= serverConfigs.size();
  }

  private void startOfServerVisit() {
    httpClient = vertx.createHttpClient()
      .setHost(serverConfig.host())
      .setPort(serverConfig.port())
      .setSSL(serverConfig.ssl())
      .setTryUseCompression(true);
    // Get the following error without turning keep alive off.  Looks like a vertx bug
    // SEVERE: Exception in Java verticle
    // java.nio.channels.ClosedChannelException
    httpClient.setKeepAlive(false);

    metrics = new JsonArray();
    servers.add(new JsonObject()
      .putString("name", serverConfig.name())
      .putArray("metrics", metrics));
    metricIndex = 0;
  }

  private void endOfServerVisit() {
    httpClient.close();
    httpClient = null;
  }

  public void dispose() {
    if (httpClient != null) {
      httpClient.close();
    }
  }

  public void addPoint(JsonObject point) {
    nextPoints.addObject(point);
  }

  public long fromTimeInMicroseconds() {
    return fromTimeInMicroseconds;
  }

  public HttpClient httpClient() {
    return httpClient;
  }

  public boolean isLastField() {
    return (fieldIndex + 1) == fieldConfigs.size();
  }

  public boolean isLastPoint() {
    return (pointIndex + 1) == currentPoints.size();
  }

  public boolean isEndOfMetric() {
    return isLastField() && isLastPoint();
  }

  public int serverCount() {
    return serverConfigs.size();
  }

  public int metricIndex() {
    return metricIndex;
  }

  public int metricCount() {
    return metricConfigs.size();
  }

  public int fieldIndex() {
    return fieldIndex;
  }

  public int fieldCount() {
    return fieldConfigs.size();
  }

  public int pointIndex() {
    return pointIndex;
  }

  public int pointCount() {
    return currentPoints.size();
  }
}
