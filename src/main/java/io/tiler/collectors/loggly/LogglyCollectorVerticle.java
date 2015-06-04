package io.tiler.collectors.loggly;

import com.google.code.regexp.Matcher;
import io.tiler.core.BaseCollectorVerticle;
import io.tiler.collectors.loggly.config.*;
import io.tiler.core.json.JsonArrayIterable;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.simondean.vertx.async.Async;
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
  private Base64.Encoder base64Encoder = Base64.getEncoder();
  private DateTimeFormatter dateTimeFormatter = ISODateTimeFormat.dateTime().withZoneUTC();

  public void start() {
    logger = container.logger();
    config = new ConfigFactory().load(container.config());
    httpClients = createHttpClients();

    final boolean[] isRunning = {true};

    collect(result -> isRunning[0] = false);

    vertx.setPeriodic(config.collectionIntervalInMilliseconds(), timerID -> {
      if (isRunning[0]) {
        logger.warn("Collection aborted as previous run still executing");
        return;
      }

      isRunning[0] = true;

      collect(aVoid -> isRunning[0] = false);
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
    Async.waterfall()
      .<JsonArray>task(taskHandler -> getExistingMetrics(taskHandler))
      .<JsonArray>task((existingMetrics, taskHandler) -> getMetrics(existingMetrics, taskHandler))
      .run(result -> {
        if (result.failed()) {
          logger.error("Failed to collect metrics", result.cause());
          handler.handle(null);
          return;
        }

        logger.error("Collection succeeded");
        handler.handle(null);
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

          if (!point.getBoolean("stable")) {
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
        metricNames.add(config.getFullMetricName(metricConfig));
      });
    });

    return metricNames;
  }

  private void getMetrics(JsonArray existingMetrics, AsyncResultHandler<JsonArray> handler) {
    getMetrics(new MetricCollectionState(logger, config, currentTimeInMicroseconds(), existingMetrics), handler);
  }

  private void getMetrics(MetricCollectionState state, AsyncResultHandler<JsonArray> handler) {
    if (!state.nextPoint()) {
      logger.info("Processed " + state.totalFieldCount() + " fields");
      handler.handle(DefaultAsyncResult.succeed(state.servers()));
      return;
    }

    String from = formatTimeInMicrosecondsAsISODateTime(state.startOfTimePeriodInMicroseconds());
    String until = formatTimeInMicrosecondsAsISODateTime(state.endOfTimePeriodInMicroseconds());

    Server serverConfig = state.serverConfig();
    StringBuilder requestUriBuilder = new StringBuilder()
      .append(serverConfig.path())
      .append("/apiv2/fields/")
      .append(urlEncode(state.fieldConfig().name()))
      .append("/?from=" + urlEncode(from) + "&until=" + urlEncode(until) + "&facet_size=2000");

    JsonObject point = state.point();

    if (point.size() > 0) {
      requestUriBuilder.append("&q=");

      String separator = "";

      for (String fieldName : point.getFieldNames()) {
        if (!fieldName.equals("count")) {
          requestUriBuilder.append(urlEncode(separator))
            .append(urlEncode(fieldName))
            .append(":")
            .append(urlEncode(point.getField(fieldName).toString()));
          separator = " ";
        }
      }
    }

    String requestUri = requestUriBuilder.toString();
    logger.info("Request URI: '" + requestUri + "'");

    HttpClientRequest request = httpClients.get(state.serverIndex()).get(requestUri, response -> {
      if (response.statusCode() != 200) {
        handler.handle(DefaultAsyncResult.fail(new Exception("Unexpected " + response.statusCode() + "response code")));
        return;
      }

      response.bodyHandler(body -> {
        String bodyString = body.toString();
        JsonObject bodyJson;

        try {
          bodyJson = new JsonObject(bodyString);
        } catch (Exception e) {
          handler.handle(DefaultAsyncResult.fail(e));
          return;
        }

        Field fieldConfig = state.fieldConfig();
        String fieldName = fieldConfig.name();

        JsonArray items = bodyJson.getArray(fieldName);
        logger.info("Received " + items.size() + " terms for field '" + fieldName + "'");

        HashMap<Object, JsonObject> fieldNewPoints = new HashMap<>();
        boolean timePeriodIsStable = state.timePeriodIsStable();

        items.forEach(itemObject -> {
          JsonObject item = (JsonObject) itemObject;
          Object term = item.getField("term");
          long count = item.getLong("count");

          if (fieldConfig.hasReplacement() && (term instanceof String)) {
            Matcher matcher = fieldConfig.replacementRegex().matcher((String) term);

            if (matcher.find()) {
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

        fieldNewPoints.values().forEach(newPoint -> {
          if (state.isLastField()) {
            newPoint.putBoolean("stable", timePeriodIsStable)
              .putNumber("time", state.startOfTimePeriodInMicroseconds());
          }

          state.addPoint(newPoint);
        });

        if (state.isLastField()) {
          JsonObject metric = state.metric();
          transformMetric(state.serverConfig(), state.metricConfig(), metric);
          saveMetrics(new JsonArray().add(metric));
        }

        getMetrics(state, handler);
      });
    });

    setBasicAuthOnRequest(serverConfig.username(), serverConfig.password(), request);
    request.setTimeout(TWO_MINUTES_IN_MILLISECONDS);
    request.exceptionHandler(cause -> handler.handle(DefaultAsyncResult.fail(cause)));
    request.end();
  }

  private String formatTimeInMicrosecondsAsISODateTime(long timeInMicroseconds) {
    return dateTimeFormatter.print(new DateTime(timeInMicroseconds / 1000, DateTimeZone.UTC));
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

  private void transformMetric(Server serverConfig, Metric metricConfig, JsonObject metric) {
    logger.info("Transforming metric");
    long metricTimestamp = currentTimeInMicroseconds();
    String serverName = serverConfig.name();
    ArrayList<Field> fieldConfigsWithExpansionRegexs = new ArrayList<>();

    metricConfig.fields().forEach(fieldConfig -> {
      if (fieldConfig.hasExpansion()) {
        fieldConfigsWithExpansionRegexs.add(fieldConfig);
      }
    });

    metric.putNumber("timestamp", metricTimestamp);

    metric.getArray("points").forEach(pointObject -> {
      JsonObject point = (JsonObject) pointObject;
      point.putString("serverName", serverName);

      fieldConfigsWithExpansionRegexs.forEach(fieldConfig -> {
        Object value = point.getValue(fieldConfig.name());

        if (value instanceof String) {
          Matcher matcher = fieldConfig.expansionRegex().matcher((String) value);

          if (matcher.find()) {
            matcher.namedGroups().entrySet().forEach(group -> point.putString(group.getKey(), group.getValue()));
          }
        }
      });
    });
  }
}
