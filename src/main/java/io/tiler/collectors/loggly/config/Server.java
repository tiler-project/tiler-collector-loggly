package io.tiler.collectors.loggly.config;

import java.util.List;

public class Server {
  private final String name;
  private final String host;
  private final Integer port;
  private final String path;
  private final boolean ssl;
  private final String username;
  private final String password;
  private final List<Metric> metrics;

  public Server(String name, String host, Integer port, String path, Boolean ssl, String username, String password, List<Metric> metrics) {
    if (host == null) {
      host = "localhost";
    }

    if (name == null) {
      name = host;
    }

    if (port == null) {
      port = 9000;
    }

    if (path == null) {
      path = "";
    }
    else if (path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }

    if (ssl == null) {
      ssl = true;
    }

    this.name = name;
    this.host = host;
    this.port = port;
    this.path = path;
    this.ssl = ssl;
    this.username = username;
    this.password = password;
    this.metrics = metrics;
  }

  public String name() {
    return name;
  }

  public String host() {
    return host;
  }

  public Integer port() {
    return port;
  }

  public String path() {
    return path;
  }

  public boolean ssl() {
    return ssl;
  }

  public String username() {
    return username;
  }

  public String password() {
    return password;
  }

  public List<Metric> metrics() {
    return metrics;
  }
}
