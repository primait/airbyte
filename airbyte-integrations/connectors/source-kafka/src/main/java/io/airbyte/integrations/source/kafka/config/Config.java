package io.airbyte.integrations.source.kafka.config;

import io.airbyte.integrations.source.kafka.MessageFormat;
import java.util.Map;

public record Config(MessageFormat format, Map<String, Object> kafkaConfig, int maxRecords, int maxRetries, int pollingTimeInMs) {

}
