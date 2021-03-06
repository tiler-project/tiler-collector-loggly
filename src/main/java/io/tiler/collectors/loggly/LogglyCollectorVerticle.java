package io.tiler.collectors.loggly;

import com.google.code.regexp.Matcher;
import io.tiler.collectors.loggly.config.*;
import io.tiler.core.BaseCollectorVerticle;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.simondean.vertx.async.Async;
import org.simondean.vertx.async.DefaultAsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;

public class LogglyCollectorVerticle extends BaseCollectorVerticle {
  private static final long TWO_MINUTES_IN_MILLISECONDS = 2 * 60 * 1000;
  private Logger logger;
  private Config config;
  private Base64.Encoder base64Encoder = Base64.getEncoder();
  private DateTimeFormatter dateTimeFormatter = ISODateTimeFormat.dateTime().withZoneUTC();

  public void start() {
    logger = container.logger();
    config = new ConfigFactory().load(container.config());

    final RunState runState = new RunState();
    runState.start();

    collect(result -> runState.stop());

    vertx.setPeriodic(config.collectionIntervalInMilliseconds(), timerID -> {
      if (runState.isRunning() && !runState.hasTimedOut()) {
        logger.warn("Collection aborted as previous run still executing");
        return;
      }

      runState.start();

      collect(aVoid -> runState.stop());
    });

    logger.info("LogglyCollectorVerticle started");
  }

  private void collect(Handler<Void> handler) {
    logger.info("Collection started");
    getMetrics(result -> {
      if (result.failed()) {
        logger.error("Failed to collect metrics", result.cause());
        handler.handle(null);
        return;
      }

      logger.info("Collection succeeded");
      handler.handle(null);
    });
  }

  private void getMetrics(AsyncResultHandler<JsonArray> handler) {
    MetricCollectionState state = new MetricCollectionState(vertx, logger, config, currentTimeInMicroseconds());

    getMetrics(
      state,
      result -> {
        state.dispose();

        handler.handle(result);
      });
  }

  private void getMetrics(MetricCollectionState state, AsyncResultHandler<JsonArray> handler) {
    if (!state.nextPoint()) {
      logger.info("Processed " + state.totalFieldCount() + " fields");
      handler.handle(DefaultAsyncResult.succeed(state.servers()));
      return;
    }

    logger.info("Server " + (state.serverIndex() + 1) + " of " + state.serverCount() + ", " +
      "metric " + (state.metricIndex() + 1) + " of " + state.metricCount() + ", " +
      "field " + (state.fieldIndex() + 1) + " of " + state.fieldCount() + ", " +
      "point " + (state.pointIndex() + 1) + " of " + state.pointCount());

    String from = formatTimeInMicrosecondsAsISODateTime(state.fromTimeInMicroseconds());

    Server serverConfig = state.serverConfig();
    StringBuilder requestUriBuilder = new StringBuilder()
      .append(serverConfig.path())
      .append("/apiv2/fields/")
      .append(urlEncode(state.fieldConfig().name()))
      .append("/?from=")
      .append(urlEncode(from))
      .append("&facet_size=2000");

    JsonObject point = state.point();
    String query = getLogglyQuery(state.metricConfig(), point);

    if (query.length() > 0) {
      requestUriBuilder.append("&q=")
        .append(query);
    }

    String requestUri = requestUriBuilder.toString();
    logger.info("Request URI: '" + requestUri + "'");

    Async.retry()
      .<JsonObject>task(taskHandler -> {
        HttpClientRequest request = state.httpClient().get(requestUri, response -> {
          if (response.statusCode() != 200) {
            taskHandler.handle(DefaultAsyncResult.fail(new Exception("Unexpected " + response.statusCode() + "response code")));
            return;
          }

          response.bodyHandler(body -> {
            String bodyString = body.toString();
            JsonObject bodyJson;

            try {
              bodyJson = new JsonObject(bodyString);
            } catch (Exception e) {
              taskHandler.handle(DefaultAsyncResult.fail(e));
              return;
            }

            taskHandler.handle(DefaultAsyncResult.succeed(bodyJson));
          });
        });

        setBasicAuthOnRequest(serverConfig.username(), serverConfig.password(), request);
        request.setTimeout(TWO_MINUTES_IN_MILLISECONDS);
        request.exceptionHandler(cause -> taskHandler.handle(DefaultAsyncResult.fail(cause)));
        request.end();
      })
      .times(state.metricConfig().retryTimes())
      .run(result -> {
        if (result.failed()) {
          handler.handle(DefaultAsyncResult.fail(result));
          return;
        }

        JsonObject body = result.result();

        Field fieldConfig = state.fieldConfig();
        String fieldName = fieldConfig.name();

        JsonArray items = body.getArray(fieldName);
        logger.info("Received " + items.size() + " terms for field '" + fieldName + "'");

        HashMap<Object, JsonObject> fieldNewPoints = new HashMap<>();

        items.forEach(itemObject -> {
          JsonObject item = (JsonObject) itemObject;
          Object term = item.getField("term");
          long count = item.getLong("count");

          if (fieldConfig.hasReplacement() && (term instanceof String)) {
            Matcher matcher = fieldConfig.replacementRegex().matcher((String) term);
            term = matcher.replaceAll(fieldConfig.replacement());
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
            newPoint.putNumber("fromTime", state.fromTimeInMicroseconds());
          }

          state.addPoint(newPoint);
        });

        if (state.isEndOfMetric()) {
          JsonObject metric = state.metric();
          transformMetric(state.serverConfig(), state.metricConfig(), metric);
          saveMetrics(new JsonArray().add(metric));
        }

        getMetrics(state, handler);
      });
  }

  private String getLogglyQuery(Metric metricConfig, JsonObject point) {
    ArrayList<String> queryParts = new ArrayList<>();

    if (metricConfig.hasQuery()) {
      queryParts.add(metricConfig.query());
    }

    for (String fieldName : point.getFieldNames()) {
      if (!fieldName.equals("count")) {
        queryParts.add(fieldName + ":" + point.getField(fieldName).toString());
      }
    }

    return urlEncode(String.join(" ", queryParts));
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
