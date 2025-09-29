package com.lbg.markets.surveillance.relay.config;

import io.smallrye.config.ConfigMapping;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

@ConfigMapping(prefix = "relay")
public interface RelayConfiguration {

    String nodeName();

    List<SourceSystem> sources();

    GcsConfig gcs();

    MonitoringConfig monitoring();

    interface SourceSystem {
        String id();
        String path();
        FileReadyStrategy readyStrategy();
        String filePattern();
        Optional<String> doneFileSuffix();
        Duration pollInterval();
        boolean enabled();
    }

    enum FileReadyStrategy {
        DONE_FILE,
        FILE_AGE,
        IMMEDIATE,
        SCHEDULED
    }

    interface GcsConfig {
        String bucket();
        String projectId();
        int uploadBufferSize();
        Duration uploadTimeout();
    }

    interface MonitoringConfig {
        String bigQueryDataset();
        String eventsTable();
        String reprocessTable();
        Duration heartbeatInterval();
    }
}