package io.airbyte.integrations.source.kafka.state;

import java.util.Map;

public record State(Map<Integer, Long> partitions) {

}
