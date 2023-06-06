package io.airbyte.integrations.source.kafka.generator;

import com.google.common.collect.AbstractIterator;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.commons.util.AutoCloseableIterators;
import io.airbyte.integrations.source.kafka.converter.Converter;
import io.airbyte.integrations.source.kafka.mediator.KafkaMediator;
import io.airbyte.integrations.source.kafka.state.StateHelper;
import io.airbyte.protocol.models.v0.AirbyteMessage;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

public class GeneratorImpl<V> implements Generator {

  private final KafkaMediator<V> mediator;
  private final Converter<V> converter;
  private final int maxRecords;

  public GeneratorImpl(KafkaMediator<V> mediator, Converter<V> converter, int maxRecords) {
    this.mediator = mediator;
    this.converter = converter;
    this.maxRecords = maxRecords;
  }

  @Override
  final public AutoCloseableIterator<AirbyteMessage> read() {

    return AutoCloseableIterators.fromIterator(new AbstractIterator<>() {

      private int totalRead = 0;
      private final Queue<AirbyteMessage> pendingMessages = new LinkedList<>();

      @Override
      protected AirbyteMessage computeNext() {

        if (this.pendingMessages.isEmpty()) {
          if (this.totalRead < GeneratorImpl.this.maxRecords) {
            List<ConsumerRecord<String, V>> batch = pullBatchFromKafka(10);
            if (!batch.isEmpty()) {
              convertToAirbyteMessagesWithState(batch);
            }
          } else {
            return endOfData();
          }
        }

        // If no more pending kafka records, close iterator
        if (this.pendingMessages.isEmpty()) {
          return endOfData();
        } else {
          return pendingMessages.poll();
        }
      }

      private void convertToAirbyteMessagesWithState(List<ConsumerRecord<String, V>> batch) {
        final Set<TopicPartition> partitions = new HashSet<>();
        batch.forEach(it -> {
          partitions.add(new TopicPartition(it.topic(), it.partition()));
          this.pendingMessages.add(GeneratorImpl.this.converter.convertToAirbyteRecord(it.topic(), it.value()));
        });
        var offsets = GeneratorImpl.this.mediator.position(partitions);
        var stateMessages = StateHelper.toAirbyteState(offsets).stream()
            .map(it -> new AirbyteMessage().withType(AirbyteMessage.Type.STATE).withState(it)).toList();
        this.pendingMessages.addAll(stateMessages);
      }

      private List<ConsumerRecord<String, V>> pullBatchFromKafka(int maxRetries) {
        List<ConsumerRecord<String, V>> batch;
        var nrOfRetries = 0;
        do {
          batch = GeneratorImpl.this.mediator.poll();
          this.totalRead += batch.size();
        } while (batch.isEmpty() && ++nrOfRetries < maxRetries);
        return batch;
      }
    });
  }
}
