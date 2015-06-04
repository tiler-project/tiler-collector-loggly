package io.tiler.collectors.loggly;

import io.tiler.collectors.loggly.config.Config;
import io.tiler.collectors.loggly.config.Field;
import io.tiler.collectors.loggly.config.Metric;
import io.tiler.collectors.loggly.config.Server;
import io.tiler.core.json.JsonArrayIterable;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;

import java.util.Iterator;
import java.util.List;

public class MetricCollectionState {
  private final Logger logger;
  private final Config config;
  private boolean initialised = false;
  private final long currentTimeInMicroseconds;
  private int totalFieldCount;
  private final JsonArray existingMetrics;
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
  private long startOfTimePeriodInMicroseconds;
  private int timePeriodIndex;
  private int timePeriodCount;
  private long startOfLastStableTimePeriodInMilliseconds;
  private boolean timePeriodIsStable;

  public MetricCollectionState(Logger logger, Config config, long currentTimeInMicroseconds, JsonArray existingMetrics) {
    this.logger = logger;
    this.config = config;
    this.currentTimeInMicroseconds = currentTimeInMicroseconds;
    this.existingMetrics = existingMetrics;
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
    logger.info("Point " + pointIndex + " of " + currentPoints.size());

    point = currentPoints.get(pointIndex);
    totalFieldCount += 1;
  }

  private void endOfPointVisit() {
  }

  private boolean nextField() {
    if (!initialised) {
      if (!nextTimePeriod()) {
        return false;
      }
    }
    else {
      endOfFieldVisit();
      fieldIndex++;
    }

    while (noMoreFields()) {
      if (!nextTimePeriod()) {
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
    logger.info("Field " + fieldIndex + " of " + fieldConfigs.size());
    currentPoints = nextPoints;
    nextPoints = new JsonArray();
    pointIndex = 0;
  }

  private void endOfFieldVisit() {
  }

  private boolean nextTimePeriod() {
    if (!initialised) {
      if (!nextMetric()) {
        return false;
      }
    }
    else {
      endOfTimePeriodVisit();
      timePeriodIndex++;
      startOfTimePeriodInMicroseconds += metricConfig.intervalInMicroseconds();
    }

    while (noMoreTimePeriods()) {
      if (!nextMetric()) {
        return false;
      }
    }

    startOfTimePeriodVisit();
    return true;
  }

  private boolean noMoreTimePeriods() {
    return timePeriodIndex >= timePeriodCount;
  }

  private void startOfTimePeriodVisit() {
    logger.info("Time period " + timePeriodIndex + " of " + timePeriodCount);
    nextPoints = new JsonArray();
    JsonObject emptyPoint = new JsonObject();
    nextPoints.addObject(emptyPoint);

    timePeriodIsStable = startOfTimePeriodInMicroseconds <= startOfLastStableTimePeriodInMilliseconds;

    fieldIndex = 0;
  }

  private void endOfTimePeriodVisit() {
    JsonArray points = metric.getArray("points");

    nextPoints.forEach(pointObject -> {
      JsonObject point = (JsonObject) pointObject;
      points.addObject(point);
    });
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
    logger.info("Metric " + metricIndex + " of " + metricConfigs.size());
    String metricName = config.getFullMetricName(metricConfig);
    metric = findMetricByNameInJsonArray(metricName, existingMetrics);
    long startOfLatestTimePeriodInMicroseconds = findStartOfPeriod(currentTimeInMicroseconds);
    long startOfFirstTimePeriodInMicroseconds;

    if (metric != null) {
      applyRetentionPeriodToPoints(currentTimeInMicroseconds, metricConfig, metric);
      startOfFirstTimePeriodInMicroseconds = findStartOfPeriod(metric.getLong("startOfLastStableTimePeriod"));
    } else {
      metric = new JsonObject()
        .putString("name", metricName)
        .putArray("points", new JsonArray());
      startOfFirstTimePeriodInMicroseconds = startOfLatestTimePeriodInMicroseconds - metricConfig.maxCatchUpPeriodInMicroseconds();
    }

    timePeriodIndex = 0;
    timePeriodCount = (int) (((startOfLatestTimePeriodInMicroseconds - startOfFirstTimePeriodInMicroseconds) / metricConfig.intervalInMicroseconds()) + 1);
    startOfLastStableTimePeriodInMilliseconds = startOfLatestTimePeriodInMicroseconds - metricConfig.stabilityPeriodInMilliseconds() - metricConfig.intervalInMicroseconds();
    metric.putNumber("startOfLastStableTimePeriod", startOfLastStableTimePeriodInMilliseconds);

    metrics.add(metric);
    startOfTimePeriodInMicroseconds = startOfFirstTimePeriodInMicroseconds;
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
    logger.info("Server " + serverIndex + " of " + serverConfigs.size());
    metrics = new JsonArray();
    servers.add(new JsonObject()
      .putString("name", serverConfig.name())
      .putArray("metrics", metrics));
    metricIndex = 0;
  }

  private void endOfServerVisit() {
  }

  private void applyRetentionPeriodToPoints(long startOfLatestPeriodInMicroseconds, Metric metricConfig, JsonObject metric) {
    long retainFromTimeInMicroseconds = startOfLatestPeriodInMicroseconds - metricConfig.retentionPeriodInMicroseconds();
    Iterator<JsonObject> pointIterator = new JsonArrayIterable<JsonObject>(metric.getArray("points")).iterator();

    while (pointIterator.hasNext()) {
      JsonObject point = pointIterator.next();

      if (point.getLong("time") < retainFromTimeInMicroseconds) {
        pointIterator.remove();
      }
    }
  }

  private long findStartOfPeriod(long timeInMicroseconds) {
    long intervalInMicroseconds = metricConfig.intervalInMicroseconds();
    return timeInMicroseconds / intervalInMicroseconds * intervalInMicroseconds;
  }

  private JsonObject findMetricByNameInJsonArray(String metricName, JsonArray metrics) {
    for (JsonObject metric : new JsonArrayIterable<JsonObject>(metrics)) {
      if (metricName.equals(metric.getString("name"))) {
        return metric;
      }
    }

    return null;
  }

  public void addPoint(JsonObject point) {
    nextPoints.addObject(point);
  }

  public long startOfTimePeriodInMicroseconds() {
    return startOfTimePeriodInMicroseconds;
  }

  public long endOfTimePeriodInMicroseconds() {
    return startOfTimePeriodInMicroseconds + metricConfig.intervalInMicroseconds();
  }

  public boolean timePeriodIsStable() {
    return timePeriodIsStable;
  }

  public boolean isLastField() {
    return (fieldIndex + 1) == fieldConfigs.size();
  }
}
