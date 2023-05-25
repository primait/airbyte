package io.airbyte.integrations.source.kafka.state;

import java.util.Map;

public record KafkaStreamState(Map<Integer, Long> partitions) {

}