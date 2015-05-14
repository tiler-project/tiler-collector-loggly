package io.tiler.collectors.loggly;

import io.tiler.collectors.loggly.config.*;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.platform.Verticle;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.*;
import java.util.stream.Collectors;

public class LogglyCollectorVerticle extends Verticle {
  private Logger logger;
  private Config config;
  private EventBus eventBus;
  private List<HttpClient> httpClients;
  private Base64.Encoder base64Encoder;

  public void start() {
    logger = container.logger();
    config = new ConfigFactory().load(container.config());
    eventBus = vertx.eventBus();
    httpClients = createHttpClients();
    base64Encoder = Base64.getEncoder();

    final boolean[] isRunning = {true};

    collect(aVoid -> {
      isRunning[0] = false;
    });

    vertx.setPeriodic(3600000, aLong -> {
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

  private void collect(Handler<Void> handler) {
    logger.info("Collection started");
    getMetrics(servers -> {
      transformMetrics(servers, metrics -> {
        publishNewMetrics(metrics, aVoid3 -> {
          logger.info("Collection finished");
          handler.handle(null);
        });
      });
    });
  }

  private void getMetrics(Handler<JsonArray> handler) {
    JsonArray servers = new JsonArray();

    config.servers().forEach(serverConfig -> {
      JsonArray metrics = new JsonArray();

      serverConfig.metrics().forEach(metricConfig -> {
        metrics.add(new JsonObject()
          .putString("name", metricConfig.name())
          .putArray("points", new JsonArray()
            .addObject(new JsonObject())));
      });

      servers.add(new JsonObject()
        .putString("name", serverConfig.name())
        .putArray("metrics", metrics));
    });

    getMetrics(0, 0, 0, 0, servers, handler);
  }

  private void getMetrics(int serverIndex, int metricIndex, int fieldIndex, int pointIndex, JsonArray servers, Handler<JsonArray> handler) {
    if (serverIndex >= config.servers().size()) {
      handler.handle(servers);
      return;
    }

    Server serverConfig = config.servers().get(serverIndex);

    if (metricIndex >= serverConfig.metrics().size()) {
      getMetrics(serverIndex + 1, 0, 0, 0, servers, handler);
      return;
    }

    Metric metricConfig = serverConfig.metrics().get(metricIndex);

    if (fieldIndex >= metricConfig.fields().size()) {
      getMetrics(serverIndex, metricIndex + 1, 0, 0, servers, handler);
      return;
    }

    JsonObject server = servers.get(serverIndex);
    JsonObject metric = server.getArray("metrics").get(metricIndex);
    JsonArray points = metric.getArray("points");

    if (pointIndex >= points.size()) {
      metric.putArray("points", metric.getArray("newPoints"));
      metric.removeField("newPoints");

      getMetrics(serverIndex, metricIndex, fieldIndex + 1, 0, servers, handler);
      return;
    }
    else if (!metric.containsField("newPoints")) {
      metric.putArray("newPoints", new JsonArray());
    }

    Field fieldConfig = metricConfig.fields().get(fieldIndex);

    StringBuilder requestUri = new StringBuilder()
      .append(serverConfig.path())
      .append("/apiv2/fields/")
      .append(urlEncode(fieldConfig.name()))
      .append("/?from=-1d&until=now&facet_size=2000");

    JsonObject point = metric.getArray("points").get(pointIndex);

    if (point.size() > 0) {
      requestUri.append("&q=");

      String separator = "";

      for (String fieldName : point.getFieldNames()) {
        requestUri.append(urlEncode(separator))
          .append(urlEncode(fieldName))
          .append(":")
          .append(urlEncode(point.getField(fieldName).toString()));
        separator = " ";
      }
    }

    HttpClientRequest request = httpClients.get(serverIndex).get(requestUri.toString(), response -> {
      response.bodyHandler(body -> {
        String fieldName = fieldConfig.name();
        String bodyString = body.toString();
        JsonObject bodyJson = new JsonObject(bodyString);

        final JsonArray newPoints = metric.getArray("newPoints");

        JsonArray items = bodyJson.getArray(fieldName);
        logger.info("Received " + items.size() + " terms for field '" + fieldName + "'");

        items.forEach(itemObject -> {
          JsonObject item = (JsonObject) itemObject;
          JsonObject newPoint = point.copy()
            .putString(fieldName, item.getField("term"));
          newPoints.addObject(newPoint);
        });

        getMetrics(serverIndex, metricIndex, fieldIndex, pointIndex + 1, servers, handler);
      });
    });

    setBasicAuthOnRequest(serverConfig.username(), serverConfig.password(), request);
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
    long metricTimestamp = getCurrentTimestampInMicroseconds();

    for (int serverIndex = 0, serverCount = config.servers().size(); serverIndex < serverCount; serverIndex++) {
      JsonObject server = servers.get(serverIndex);
      Server serverConfig = config.servers().get(serverIndex);

      String serverName = server.getString("name");

      for (int metricIndex = 0, metricCount = serverConfig.metrics().size(); metricIndex < metricCount; metricIndex++) {
        JsonObject metric = server.getArray("metrics").get(metricIndex);
        Metric metricConfig = serverConfig.metrics().get(metricIndex);

        Map<String, Field> fieldConfigsWithExpansionPatterns = new HashMap<>();

        metricConfig.fields().forEach(fieldConfig -> {
          if (fieldConfig.hasExpansionPattern()) {
            fieldConfigsWithExpansionPatterns.put(fieldConfig.name(), fieldConfig);
          }
        });

        metric.putNumber("timestamp", metricTimestamp);

        metric.getArray("points").forEach(pointObject -> {
          JsonObject point = (JsonObject) pointObject;
          point.putNumber("time", metricTimestamp)
            .putString("serverName", serverName);

          fieldConfigsWithExpansionPatterns.entrySet().forEach(entry -> {
            // TODO Implement code to extract groups
          });
        });

        newMetrics.add(metric);
      }
    }

    handler.handle(newMetrics);
  }

  private long getCurrentTimestampInMicroseconds() {
    return System.currentTimeMillis() * 1000;
  }

  private void publishNewMetrics(JsonArray metrics, Handler<Void> handler) {
    logger.info("Publishing metrics to event bus");
    JsonObject message = new JsonObject()
      .putArray("metrics", metrics);
    eventBus.publish("io.squarely.vertxspike.metrics", message);
    handler.handle(null);
  }
}
