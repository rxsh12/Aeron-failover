package demo.aeron;

import io.aeron.CommonContext;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

final class DemoConfig {
    private final Properties values;

    private DemoConfig(Properties values) {
        this.values = values;
    }

    static DemoConfig load() throws IOException {
        String path = System.getProperty("aeron.demo.config");
        if (path == null || path.isBlank()) {
            throw new IllegalStateException("Configuration file path was not supplied by run.sh");
        }
        Properties values = new Properties();
        try (InputStream input = Files.newInputStream(Path.of(path))) {
            values.load(input);
        }
        return new DemoConfig(values);
    }

    String string(String key) {
        String value = values.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required configuration: " + key);
        }
        return value.trim();
    }

    String string(String key, String defaultValue) {
        String value = values.getProperty(key);
        return value == null || value.isBlank() ? defaultValue : value.trim();
    }

    int integer(String key) {
        try {
            return Integer.parseInt(string(key));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Configuration " + key + " must be an integer", e);
        }
    }

    int integer(String key, int defaultValue) {
        String value = values.getProperty(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Configuration " + key + " must be an integer", e);
        }
    }

    long longValue(String key) {
        try {
            return Long.parseLong(string(key));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Configuration " + key + " must be an integer", e);
        }
    }

    long longValue(String key, long defaultValue) {
        String value = values.getProperty(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Configuration " + key + " must be an integer", e);
        }
    }

    boolean bool(String key, boolean defaultValue) {
        String value = values.getProperty(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        if ("true".equalsIgnoreCase(value.trim())) {
            return true;
        }
        if ("false".equalsIgnoreCase(value.trim())) {
            return false;
        }
        throw new IllegalArgumentException("Configuration " + key + " must be true or false");
    }

    Path path(String key) {
        return Path.of(string(key));
    }

    List<String> clusterHosts() {
        List<String> hosts = Arrays.stream(string("cluster.hosts").split(",", -1))
            .map(String::trim)
            .toList();
        if (hosts.stream().anyMatch(String::isEmpty)) {
            throw new IllegalArgumentException("Configuration cluster.hosts contains an empty host");
        }
        return hosts;
    }

    String aeronDirectoryPrefix() {
        return string("aeron.directory-prefix", CommonContext.getAeronDirectoryName());
    }
}
