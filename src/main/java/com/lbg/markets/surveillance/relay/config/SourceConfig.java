package com.lbg.markets.surveillance.relay.config;

import io.smallrye.config.ConfigMapping;

import java.time.Duration;
import java.util.List;

@ConfigMapping(prefix = "relay")
public interface SourceConfig {

    List<Source> sources();

    interface Source {
        String id();

        String path();

        String filePattern();

        String readyStrategy();

        Duration stabilityPeriod();

        Duration pollInterval();

        boolean enabled();

        default String getId() {
            return id();
        }

        default String getPath() {
            return path();
        }

        default String getFilePattern() {
            return filePattern();
        }

        default String getReadyStrategy() {
            return readyStrategy();
        }

        default Duration getStabilityPeriod() {
            return stabilityPeriod();
        }

        default Duration getPollInterval() {
            return pollInterval();
        }

        default boolean isEnabled() {
            return enabled();
        }
    }
}