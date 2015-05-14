package io.tiler.collectors.loggly.config;

import java.util.List;

public class Metric {
  private final String name;
  private final List<Field> fields;

  public Metric(String name, List<Field> fields) {
    this.name = name;
    this.fields = fields;
  }

  public String name() {
    return name;
  }

  public List<Field> fields() {
    return fields;
  }
}
