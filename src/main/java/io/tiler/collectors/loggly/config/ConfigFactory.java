package io.tiler.collectors.loggly.config;

import com.google.code.regexp.Pattern;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;

public class ConfigFactory {
  public Config load(JsonObject config) {
    return new Config(
      config.getString("collectionInterval"),
      getServers(config),
      config.getString("metricNamePrefix"));
  }

  private List<Server> getServers(JsonObject config) {
    JsonArray servers = config.getArray("servers");
    ArrayList<Server> loadedServers = new ArrayList<>();

    if (servers == null) {
      return loadedServers;
    }

    servers.forEach(serverObject -> {
      JsonObject server = (JsonObject) serverObject;
      loadedServers.add(getServer(server));
    });

    return loadedServers;
  }

  private Server getServer(JsonObject server) {
    return new Server(
      server.getString("name"),
      server.getString("host"),
      server.getInteger("port"),
      server.getString("path"),
      server.getBoolean("ssl"),
      server.getString("username"),
      server.getString("password"),
      getServerMetrics(server));
  }

  private List<Metric> getServerMetrics(JsonObject server) {
    JsonArray metrics = server.getArray("metrics");
    ArrayList<Metric> loadedMetrics = new ArrayList<>();

    if (metrics == null) {
      return loadedMetrics;
    }

    metrics.forEach(metricObject -> {
      JsonObject metric = (JsonObject) metricObject;
      loadedMetrics.add(getMetric(metric));
    });

    return loadedMetrics;
  }

  private Metric getMetric(JsonObject metric) {
    return new Metric(
      metric.getString("name"),
      metric.getString("interval"),
      metric.getString("retentionPeriod"),
      metric.getString("maxCatchUpPeriod"),
      metric.getString("stabilityPeriod"),
      metric.getInteger("retryTimes"),
      metric.getString("query"),
      getMetricFields(metric));
  }

  private List<Field> getMetricFields(JsonObject metric) {
    JsonArray fields = metric.getArray("fields");
    ArrayList<Field> loadedFields = new ArrayList<>();

    if (fields == null) {
      return loadedFields;
    }

    fields.forEach(fieldObject -> loadedFields.add(getField(fieldObject)));

    return loadedFields;
  }

  private Field getField(Object fieldObject) {
    if (fieldObject instanceof String) {
      return new Field((String) fieldObject, null, null, null);
    }
    
    JsonObject field = (JsonObject) fieldObject;
    return new Field(
      field.getString("name"),
      compileRegex(field.getObject("expansionRegex")),
      compileRegex(field.getObject("replacementRegex")),
      field.getString("replacement"));
  }

  private Pattern compileRegex(JsonObject value) {
    if (value == null) {
      return null;
    }

    return Pattern.compile(value.getString("pattern"));
  }
}
