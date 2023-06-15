package io.airbyte.integrations.source.kafka.config;

import io.airbyte.integrations.source.kafka.MessageFormat;
import java.util.Map;

public record SourceConfig(MessageFormat format, KafkaConfig kafkaConfig, int maxRecords, int maxRetries, int pollingTimeInMs) {

}
