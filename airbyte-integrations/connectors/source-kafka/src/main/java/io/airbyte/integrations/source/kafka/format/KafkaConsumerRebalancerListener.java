package io.airbyte.integrations.source.kafka.format;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class KafkaConsumerRebalancerListener<K, V> implements ConsumerRebalanceListener {

  public KafkaConsumerRebalancerListener(final KafkaConsumer<K, V> consumer, final Map<TopicPartition, Long> positions) {
    this.consumer = consumer;
    this.positions = positions;
  }

  @Override
  public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {

  }

  @Override
  public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
    partitions.forEach(partition -> Optional.ofNullable(positions.get(partition)).ifPresent(position -> consumer.seek(partition, position)));
  }

  private final KafkaConsumer<K, V> consumer;
  private final Map<TopicPartition, Long> positions;
}
