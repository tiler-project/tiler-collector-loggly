package io.tiler.collectors.loggly;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;

public class RunState {
  private static final Duration timeout = Duration.standardHours(1);
  private boolean isRunning = false;
  private DateTime startDateTime;

  private DateTime now() {
    return DateTime.now(DateTimeZone.UTC);
  }

  public boolean isRunning() {
    return isRunning;
  }

  public DateTime startDateTime() {
    return startDateTime;
  }

  public void start() {
    isRunning = true;
    startDateTime = now();
  }

  public void stop() {
    isRunning = false;
    startDateTime = null;
  }

  public boolean hasTimedOut() {
    Duration elapsed = new Duration(startDateTime, now());
    return elapsed.compareTo(timeout) >= 0;
  }
}
