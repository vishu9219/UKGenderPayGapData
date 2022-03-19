package com.vsrivastava.loadtobigquery.dependency;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class DependencyProperties {
    private static Properties properties;

    private DependencyProperties() {
    }

    public static Properties getProperties() throws IOException {
        if (properties == null) {
            synchronized (DependencyProperties.class) {
                if (properties == null) {
                    try (InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("application.properties")) {
                        properties = new Properties();
                        properties.load(inputStream);
                    }
                }
            }
        }
        return properties;
    }
}
