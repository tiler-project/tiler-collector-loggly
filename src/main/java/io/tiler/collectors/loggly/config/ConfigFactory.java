package io.tiler.collectors.loggly.config;

import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class ConfigFactory {
  private static final long ONE_HOUR_IN_MILLISECONDS = 60 * 60 * 1000l;

  public Config load(JsonObject config) {
    return new Config(
      getCollectionIntervalInMilliseconds(config),
      getServers(config),
      getMetricNamePrefix(config));
  }

  private long getCollectionIntervalInMilliseconds(JsonObject config) {
    return config.getLong("collectionIntervalInMilliseconds", ONE_HOUR_IN_MILLISECONDS);
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
      getMetricFields(metric));
  }
  
  private String getMetricName(JsonObject metric) {
    return metric.getString("name");
  }

  private List<Field> getMetricFields(JsonObject metric) {
    JsonArray fields = metric.getArray("fields");
    ArrayList<Field> loadedFields = new ArrayList<>();

    if (fields == null) {
      return loadedFields;
    }

    fields.forEach(fieldObject -> {
      loadedFields.add(getField(fieldObject));
    });

    return loadedFields;
  }

  private Field getField(Object fieldObject) {
    if (fieldObject instanceof String) {
      return new Field((String) fieldObject, null);
    }
    
    JsonObject field = (JsonObject) fieldObject;
    return new Field(
      getFieldName(field),
      getFieldExpansionPattern(field));
  }

  private String getFieldName(JsonObject field) {
    return field.getString("name");
  }

  private Pattern getFieldExpansionPattern(JsonObject field) {
    if (!field.containsField("expansionPattern")) {
      return null;
    }

    return Pattern.compile(field.getString("expansionPattern"));
  }
}
