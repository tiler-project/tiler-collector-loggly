package io.tiler.collectors.loggly.config;

import java.util.regex.Pattern;

public class Field {
  private final String name;
  private final Pattern expansionPattern;

  public Field(String name, Pattern expansionPattern) {
    this.name = name;
    this.expansionPattern = expansionPattern;
  }

  public String name() {
    return name;
  }

  public Pattern expansionPattern() {
    return expansionPattern;
  }

  public boolean hasExpansionPattern() {
    return expansionPattern != null;
  }
}
