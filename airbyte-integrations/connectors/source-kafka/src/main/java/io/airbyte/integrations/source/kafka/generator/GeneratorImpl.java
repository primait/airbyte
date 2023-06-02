package io.airbyte.integrations.source.kafka.generator;

import com.google.common.collect.AbstractIterator;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.commons.util.AutoCloseableIterators;
import io.airbyte.integrations.source.kafka.converter.Converter;
import io.airbyte.integrations.source.kafka.mediator.KafkaMediator;
import io.airbyte.integrations.source.kafka.model.StateHelper;
import io.airbyte.protocol.models.v0.AirbyteMessage;
import io.airbyte.protocol.models.v0.AirbyteStateMessage;
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
      private final Set<TopicPartition> partitions = new HashSet<>();
      final Queue<ConsumerRecord<String, V>> pendingKafkaRecords = new LinkedList<>();
      final Queue<AirbyteStateMessage> pendingStateMessages = new LinkedList<>();

      @Override
      protected AirbyteMessage computeNext() {

        // If no pending records
        //
        if (this.pendingKafkaRecords.isEmpty()) {
          // Check if we emit messages from any partition. If we did, prepare state messages; if not, load a new batch
          //
          if (this.partitions.isEmpty()) {
            if (this.totalRead < GeneratorImpl.this.maxRecords) {
              List<ConsumerRecord<String, V>> batch;
              var nrOfRetries = 0;
              do {
                batch = GeneratorImpl.this.mediator.poll();
                this.totalRead += batch.size();
                this.pendingKafkaRecords.addAll(batch);
              } while (batch.size() == 0 && ++nrOfRetries < 10);
            } else {
              return endOfData();
            }
          } else {
            var positions = GeneratorImpl.this.mediator.position(this.partitions);
            this.pendingStateMessages.addAll(StateHelper.toAirbyteState(positions));
            this.partitions.clear();
          }
        }

        // Emit any pending state messages first
        if (!this.pendingStateMessages.isEmpty()) {
          return new AirbyteMessage().withType(AirbyteMessage.Type.STATE).withState(this.pendingStateMessages.poll());
        }

        // If no more pending kafka records, finish
        if (this.pendingKafkaRecords.isEmpty()) {
          return endOfData();
        } else {
          var message = this.pendingKafkaRecords.poll();
          this.partitions.add(new TopicPartition(message.topic(), message.partition()));
          return GeneratorImpl.this.converter.convertToAirbyteRecord(message.topic(), message.value());
        }
      }
    });
  }
}
