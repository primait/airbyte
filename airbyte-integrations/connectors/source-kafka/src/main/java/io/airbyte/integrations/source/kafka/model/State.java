package io.airbyte.integrations.source.kafka.model;

import java.util.Map;

public record State(Map<Integer, Long> partitions) {

}
