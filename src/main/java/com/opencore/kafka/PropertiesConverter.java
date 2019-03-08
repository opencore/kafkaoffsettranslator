package com.opencore.kafka;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

public class PropertiesConverter implements CommandLine.ITypeConverter<Properties> {
  private static final Logger logger = LoggerFactory.getLogger(PropertiesConverter.class);

  public PropertiesConverter() {
  }

  @Override
  public Properties convert(String fileName) throws Exception {
    Properties result = new Properties();
    File propertiesFile = new File(fileName);

    try {
      result.load(new FileInputStream(propertiesFile));
    } catch (IOException e) {
      logger.error("Error loading properties from file " + fileName + ": " + e.getMessage());
      System.exit(-1);
    }
    return result;
  }
}
