package com.opencore.kafka;

import picocli.CommandLine;


public class OffsetTranslatorCommand {
  public static void main(String[] args) {
    CommandLine.call(new KafkaOffsetTranslator(), args);
  }
}
