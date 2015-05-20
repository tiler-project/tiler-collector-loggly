package io.tiler.collectors.loggly.config;

import com.google.code.regexp.Pattern;

public class Field {
  private final String name;
  private final Pattern expansionRegex;
  private final Pattern replacementRegex;
  private final String replacement;

  public Field(String name, Pattern expansionRegex, Pattern replacementRegex, String replacement) {
    this.name = name;
    this.expansionRegex = expansionRegex;
    this.replacementRegex = replacementRegex;
    this.replacement = replacement;
  }

  public String name() {
    return name;
  }

  public boolean hasExpansion() {
    return expansionRegex != null;
  }

  public Pattern expansionRegex() {
    return expansionRegex;
  }

  public boolean hasReplacement() {
    return replacementRegex != null;
  }

  public Pattern replacementRegex() {
    return replacementRegex;
  }

  public String replacement() {
    return replacement;
  }
}
