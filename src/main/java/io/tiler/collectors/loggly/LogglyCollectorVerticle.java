package io.tiler.collectors.loggly;

import com.google.code.regexp.Matcher;
import io.tiler.BaseCollectorVerticle;
import io.tiler.collectors.loggly.config.*;
import io.tiler.json.JsonArrayIterable;
import org.simondean.vertx.async.DefaultAsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.*;
import java.util.stream.Collectors;

public class LogglyCollectorVerticle extends BaseCollectorVerticle {
  private static final long TWO_MINUTES_IN_MILLISECONDS = 2 * 60 * 1000;
  private Logger logger;
  private Config config;
  private List<HttpClient> httpClients;
  private Base64.Encoder base64Encoder;

  public void start() {
    logger = container.logger();
    config = new ConfigFactory().load(container.config());
    httpClients = createHttpClients();
    base64Encoder = Base64.getEncoder();

    final boolean[] isRunning = {true};

    collect(aVoid -> {
      isRunning[0] = false;
    });

    vertx.setPeriodic(config.collectionIntervalInMilliseconds(), aLong -> {
      if (isRunning[0]) {
        logger.info("Collection aborted as previous run still executing");
        return;
      }

      isRunning[0] = true;

      collect(aVoid -> {
        isRunning[0] = false;
      });
    });

    logger.info("LogglyCollectorVerticle started");
  }

  private List<HttpClient> createHttpClients() {
    return config.servers()
      .stream()
      .map(server -> {
        HttpClient httpClient = vertx.createHttpClient()
          .setHost(server.host())
          .setPort(server.port())
          .setSSL(server.ssl())
          .setTryUseCompression(true);
        // Get the following error without turning keep alive off.  Looks like a vertx bug
        // SEVERE: Exception in Java verticle
        // java.nio.channels.ClosedChannelException
        httpClient.setKeepAlive(false);
        return httpClient;
      })
      .collect(Collectors.toList());
  }

  private void collect(AsyncResultHandler<Void> handler) {
    logger.info("Collection started");
    getExistingMetrics(result -> {
      if (result.failed()) {
        handler.handle(DefaultAsyncResult.fail(result.cause()));
        return;
      }

      JsonArray existingMetrics = result.result();

      getMetrics(existingMetrics, result2 -> {
        if (result2.failed()) {
          handler.handle(DefaultAsyncResult.fail(result.cause()));
          return;
        }

        JsonArray servers = result2.result();

        transformMetrics(servers, metrics -> {
          saveMetrics(metrics);
          logger.info("Collection finished");
          handler.handle(null);
        });
      });
    });
  }

  private void getExistingMetrics(AsyncResultHandler<JsonArray> handler) {
    getExistingMetrics(getMetricNames(), result -> {
      if (result.failed()) {
        handler.handle(result);
        return;
      }

      JsonArray existingMetrics = result.result();

      for (JsonObject metric : new JsonArrayIterable<JsonObject>(existingMetrics)) {
        Iterator<JsonObject> pointIterator = new JsonArrayIterable<JsonObject>(metric.getArray("points")).iterator();

        while (pointIterator.hasNext()) {
          JsonObject point = pointIterator.next();

          if (point.getBoolean("complete") == false) {
            pointIterator.remove();
          }
        }
      }

      handler.handle(DefaultAsyncResult.succeed(existingMetrics));
    });
  }

  private List<String> getMetricNames() {
    ArrayList<String> metricNames = new ArrayList<>();

    config.servers().forEach(serverConfig -> {
      serverConfig.metrics().forEach(metricConfig -> {
        metricNames.add(getMetricName(metricConfig));
      });
    });

    return metricNames;
  }

  private String getMetricName(Metric metricConfig) {
    return config.metricNamePrefix() + metricConfig.name();
  }

  private void getMetrics(JsonArray existingMetrics, AsyncResultHandler<JsonArray> handler) {
    getMetrics(new MetricCollectionState(existingMetrics), handler);
  }

  private void getMetrics(MetricCollectionState state, AsyncResultHandler<JsonArray> handler) {
    if (!state.nextPoint()) {
      logger.info("Processed " + state.totalFieldCount() + " fields");
      handler.handle(DefaultAsyncResult.succeed(state.servers()));
      return;
    }

    Server serverConfig = state.serverConfig();
    StringBuilder requestUri = new StringBuilder()
      .append(serverConfig.path())
      .append("/apiv2/fields/")
      .append(urlEncode(state.fieldConfig().name()))
      .append("/?from=-1d&until=now&facet_size=2000");

    JsonObject point = state.point();

    if (point.size() > 0) {
      requestUri.append("&q=");

      String separator = "";

      for (String fieldName : point.getFieldNames()) {
        if (!fieldName.equals("count")) {
          requestUri.append(urlEncode(separator))
            .append(urlEncode(fieldName))
            .append(":")
            .append(urlEncode(point.getField(fieldName).toString()));
          separator = " ";
        }
      }
    }

    HttpClientRequest request = state.httpClient().get(requestUri.toString(), response -> {
      response.bodyHandler(body -> {
        Field fieldConfig = state.fieldConfig();
        String fieldName = fieldConfig.name();
        String bodyString = body.toString();
        JsonObject bodyJson = new JsonObject(bodyString);

        JsonArray items = bodyJson.getArray(fieldName);
        logger.info("Received " + items.size() + " terms for field '" + fieldName + "'");

        HashMap<Object, JsonObject> fieldNewPoints = new HashMap<>();

        items.forEach(itemObject -> {
          JsonObject item = (JsonObject) itemObject;
          Object term = item.getField("term");
          long count = item.getLong("count");

          if (fieldConfig.hasReplacement() && (term instanceof String)) {
            Matcher matcher = fieldConfig.replacementRegex().matcher((String) term);

            if (matcher.matches()) {
              term = matcher.replaceAll(fieldConfig.replacement());
            }
          }

          JsonObject newPoint = fieldNewPoints.get(term);

          if (newPoint != null) {
            count += newPoint.getLong("count");
            newPoint.putNumber("count", count);
          } else {
            newPoint = point.copy()
              .putValue(fieldName, term)
              .putNumber("count", count);
            fieldNewPoints.put(term, newPoint);
          }
        });

        logger.info("Left with " + fieldNewPoints.size() + " terms for field '" + fieldName + "' after replacement");

        fieldNewPoints.values().forEach(state::addPoint);

        getMetrics(state, handler);
      });
    });

    setBasicAuthOnRequest(serverConfig.username(), serverConfig.password(), request);
    request.setTimeout(TWO_MINUTES_IN_MILLISECONDS);
    request.exceptionHandler(cause -> handler.handle(DefaultAsyncResult.fail(cause)));
    request.end();
  }

  private void setBasicAuthOnRequest(String username, String password, HttpClientRequest request) {
    request.putHeader(
      "Authorization",
      "Basic " + base64Encoder.encodeToString((username + ":" + password).getBytes()));
  }

  private String urlEncode(String value) {
    try {
      return URLEncoder.encode(value, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  private void transformMetrics(JsonArray servers, Handler<JsonArray> handler) {
    logger.info("Transforming metrics");
    JsonArray newMetrics = new JsonArray();
    long metricTimestamp = currentTimeInMicroseconds();

    for (int serverIndex = 0, serverCount = config.servers().size(); serverIndex < serverCount; serverIndex++) {
      JsonObject server = servers.get(serverIndex);
      Server serverConfig = config.servers().get(serverIndex);

      String serverName = server.getString("name");

      for (int metricIndex = 0, metricCount = serverConfig.metrics().size(); metricIndex < metricCount; metricIndex++) {
        JsonObject metric = server.getArray("metrics").get(metricIndex);
        Metric metricConfig = serverConfig.metrics().get(metricIndex);

        ArrayList<Field> fieldConfigsWithExpansionRegexs = new ArrayList<>();

        metricConfig.fields().forEach(fieldConfig -> {
          if (fieldConfig.hasExpansion()) {
            fieldConfigsWithExpansionRegexs.add(fieldConfig);
          }
        });

        metric.putNumber("timestamp", metricTimestamp);

        metric.getArray("points").forEach(pointObject -> {
          JsonObject point = (JsonObject) pointObject;
          point.putNumber("time", metricTimestamp)
            .putString("serverName", serverName);

          fieldConfigsWithExpansionRegexs.forEach(fieldConfig -> {
            Object value = point.getString(fieldConfig.name());

            if (value instanceof String) {
              Matcher matcher = fieldConfig.expansionRegex().matcher((String) value);

              if (matcher.matches()) {
                matcher.namedGroups().entrySet().forEach(group -> point.putString(group.getKey(), group.getValue()));
              }
            }
          });
        });

        newMetrics.add(metric);
      }
    }

    handler.handle(newMetrics);
  }

  private class MetricCollectionState {
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

    public MetricCollectionState(JsonArray existingMetrics) {
      currentTimeInMicroseconds = currentTimeInMicroseconds();
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

    public Field fieldConfig() {
      return fieldConfig;
    }

    public JsonArray servers() {
      return servers;
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
      }

      while (pointIndex >= currentPoints.size()) {
        if (!nextField()) {
          return false;
        }
      }

      logger.info("Point " + pointIndex + " of " + currentPoints.size());

      point = currentPoints.get(pointIndex);
      totalFieldCount += 1;
      return true;
    }

    private boolean nextField() {
      if (!initialised) {
        if (!nextMetric()) {
          return false;
        }
      }
      else {
        fieldIndex++;

        if (fieldIndex >= fieldConfigs.size()) {
          JsonArray points = metric.getArray("points");

          nextPoints.forEach(pointObject -> {
            JsonObject point = (JsonObject) pointObject;
            points.addObject(point);
          });
        }
      }

      while (fieldIndex >= fieldConfigs.size()) {
        if (!nextMetric()) {
          return false;
        }
      }

      logger.info("Field " + fieldIndex + " of " + fieldConfigs.size());

      fieldConfig = fieldConfigs.get(fieldIndex);
      currentPoints = nextPoints;
      nextPoints = new JsonArray();

      pointIndex = 0;
      return true;
    }

    private boolean nextMetric() {
      if (!initialised) {
        if (!nextServer()) {
          return false;
        }
      }
      else {
        metricIndex++;
      }

      while (metricIndex >= metricConfigs.size()) {
        if (!nextServer()) {
          return false;
        }
      }

      logger.info("Metric " + metricIndex + " of " + metricConfigs.size());

      metricConfig = metricConfigs.get(metricIndex);
      fieldConfigs = metricConfig.fields();

      String metricName = getMetricName(metricConfig);
      metric = findMetricByNameInJsonArray(metricName, existingMetrics);

      if (metric != null) {
        applyRetentionPeriodToPoints(currentTimeInMicroseconds, metricConfig, metric);
      } else {
        metric = new JsonObject()
          .putString("name", metricName)
          .putArray("points", new JsonArray());
      }

      metrics.add(metric);
      nextPoints = new JsonArray();
      JsonObject emptyPoint = new JsonObject();
      nextPoints.addObject(emptyPoint);

      fieldIndex = 0;
      return true;
    }

    private boolean nextServer() {
      if (!initialised) {
        serverIndex = 0;
        initialised = true;
      }
      else {
        serverIndex++;
      }

      if (serverIndex >= serverConfigs.size()) {
        return false;
      }

      logger.info("Server " + serverIndex + " of " + serverConfigs.size());

      serverConfig = serverConfigs.get(serverIndex);
      metricConfigs = serverConfig.metrics();
      metrics = new JsonArray();
      servers.add(new JsonObject()
        .putString("name", serverConfig.name())
        .putArray("metrics", metrics));
      metricIndex = 0;
      return true;
    }

    public HttpClient httpClient() {
      return httpClients.get(serverIndex);
    }

    private void applyRetentionPeriodToPoints(long currentTimeInMicroseconds, Metric metricConfig, JsonObject metric) {
      long startOfLatestPeriodInMicroseconds = findStartOfPeriod(currentTimeInMicroseconds, metricConfig.intervalInMicroseconds());
      long retainFromTimeInMicroseconds = startOfLatestPeriodInMicroseconds - metricConfig.retentionPeriodInMicroseconds();
      Iterator<JsonObject> pointIterator = new JsonArrayIterable<JsonObject>(metric.getArray("points")).iterator();

      while (pointIterator.hasNext()) {
        JsonObject point = pointIterator.next();

        if (point.getLong("time") < retainFromTimeInMicroseconds) {
          pointIterator.remove();
        }
      }
    }

    private long findStartOfPeriod(long timeInMicroseconds, long intervalInMicroseconds) {
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
  }
}
