package io.tiler.collectors.loggly.config;

import com.google.code.regexp.Pattern;
import io.tiler.time.TimePeriodParser;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;

public class ConfigFactory {
  public Config load(JsonObject config) {
    return new Config(
      getCollectionIntervalInMilliseconds(config),
      getServers(config),
      getMetricNamePrefix(config));
  }

  private long getCollectionIntervalInMilliseconds(JsonObject config) {
    return TimePeriodParser.parseTimePeriodToMilliseconds(config.getString("collectionIntervalInMilliseconds", "1h"));
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
      getServerName(server),
      getServerHost(server),
      getServerPort(server),
      getServerPath(server),
      getServerSsl(server),
      getServerUsername(server),
      getServerPassword(server),
      getServerMetrics(server));
  }
  
  private String getServerName(JsonObject server) {
    return server.getString("name");
  }

  private boolean getServerSsl(JsonObject server) {
    return server.getBoolean("ssl", false);
  }

  private int getServerPort(JsonObject server) {
    return server.getInteger("port", 9000);
  }

  private String getServerPath(JsonObject server) {
    return server.getString("path", "");
  }

  private String getServerHost(JsonObject server) {
    return server.getString("host", "localhost");
  }

  private String getMetricNamePrefix(JsonObject config) {
    return config.getString("metricNamePrefix", "loggly");
  }

  private String getServerUsername(JsonObject server) {
    return server.getString("username");
  }

  private String getServerPassword(JsonObject server) {
    return server.getString("password");
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
      getMetricName(metric),
      getMetricInterval(metric),
      getMetricRetentionPeriod(metric),
      getMetricMaxCatchUpPeriod(metric),
      getMetricFields(metric));
  }

  private String getMetricName(JsonObject metric) {
    return metric.getString("name");
  }

  private String getMetricInterval(JsonObject metric) {
    return metric.getString("interval", "1h");
  }

  private String getMetricRetentionPeriod(JsonObject metric) {
    return metric.getString("retentionPeriod", "1d");
  }

  private String getMetricMaxCatchUpPeriod(JsonObject metric) {
    return metric.getString("maxCatchUpPeriod", "1d");
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
      getFieldName(field),
      getFieldExpansionRegex(field),
      getFieldReplacementRegex(field),
      getFieldReplacement(field));
  }

  private String getFieldName(JsonObject field) {
    return field.getString("name");
  }

  private Pattern getFieldExpansionRegex(JsonObject field) {
    return compileRegex(field.getObject("expansionRegex"));
  }

  private Pattern getFieldReplacementRegex(JsonObject field) {
    return compileRegex(field.getObject("replacementRegex"));
  }

  private String getFieldReplacement(JsonObject field) {
    return field.getString("replacement");
  }

  private Pattern compileRegex(JsonObject value) {
    if (value == null) {
      return null;
    }

    return Pattern.compile(value.getString("pattern"));
  }
}
