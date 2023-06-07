/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.kafka.mediator;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.integrations.source.kafka.KafkaConsumerRebalanceListener;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaMediatorImpl<V> implements KafkaMediator<V> {

  private final KafkaConsumer<String, V> consumer;
  private final int pollingTimeInMs;

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMediatorImpl.class);

  public KafkaMediatorImpl(KafkaConsumer<String, V> consumer, KafkaConsumerRebalanceListener listener, JsonNode config) {
    final JsonNode subscription = config.get("subscription");
    LOGGER.info("Kafka subscribe method: {}", subscription.toString());
    switch (subscription.get("subscription_type").asText()) {
      case "subscribe" -> {
        final String topicPattern = subscription.get("topic_pattern").asText();
        consumer.subscribe(Pattern.compile(topicPattern), listener);
      }
      case "assign" -> {
        final String topicPartitions = subscription.get("topic_partitions").asText();
        final String[] topicPartitionsStr = topicPartitions.replaceAll("\\s+", "").split(",");
        final List<TopicPartition> topicPartitionList = Arrays.stream(topicPartitionsStr).map(topicPartition -> {
          final String[] pair = topicPartition.split(":");
          return new TopicPartition(pair[0], Integer.parseInt(pair[1]));
        }).collect(Collectors.toList());
        LOGGER.info("Topic-partition list: {}", topicPartitionList);
        consumer.assign(topicPartitionList);
        topicPartitionList.forEach(partition -> Optional.ofNullable(listener.getInitialPositions().get(partition))
            .ifPresent(offset -> consumer.seek(partition, offset)));
      }
    }

    this.consumer = consumer;
    this.pollingTimeInMs = config.has("polling_time") ? config.get("polling_time").intValue() : 100;
  }

  @Override
  public List<ConsumerRecord<String, V>> poll() {
    List<ConsumerRecord<String, V>> output = new ArrayList<>();
    consumer.poll(Duration.of(this.pollingTimeInMs, ChronoUnit.MILLIS)).forEach(output::add);
    return output;
  }

  @Override
  public Map<TopicPartition, Long> position(Set<TopicPartition> partitions) {
    return partitions.stream()
        .map(it -> Map.entry(it, consumer.position(it)))
        .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
  }

}
