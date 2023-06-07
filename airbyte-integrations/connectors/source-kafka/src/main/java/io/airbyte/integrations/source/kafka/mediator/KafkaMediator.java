/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.kafka.mediator;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

public interface KafkaMediator<V> {

  List<ConsumerRecord<String, V>> poll();

  Map<TopicPartition, Long> position(Set<TopicPartition> partitions);

}
