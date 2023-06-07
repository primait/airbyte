/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

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
  private final int maxRetries;

  public GeneratorImpl(KafkaMediator<V> mediator, Converter<V> converter, int maxRecords, int maxRetries) {
    this.mediator = mediator;
    this.converter = converter;
    this.maxRecords = maxRecords;
    this.maxRetries = maxRetries;
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
            List<ConsumerRecord<String, V>> batch = pullBatchFromKafka();
            if (!batch.isEmpty()) {
              this.totalRead += batch.size();
              this.pendingMessages.addAll(convertToAirbyteMessagesWithState(batch));
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

      private List<AirbyteMessage> convertToAirbyteMessagesWithState(List<ConsumerRecord<String, V>> batch) {
        final Set<TopicPartition> partitions = new HashSet<>();
        batch.forEach(it -> {
          partitions.add(new TopicPartition(it.topic(), it.partition()));
          this.pendingMessages.add(
              new AirbyteMessage()
                  .withType(AirbyteMessage.Type.RECORD).withRecord(GeneratorImpl.this.converter.convertToAirbyteRecord(it.topic(), it.value()))
          );
        });
        var offsets = GeneratorImpl.this.mediator.position(partitions);
        return StateHelper.toAirbyteState(offsets).stream()
            .map(it -> new AirbyteMessage().withType(AirbyteMessage.Type.STATE).withState(it)).toList();
      }

      private List<ConsumerRecord<String, V>> pullBatchFromKafka() {
        List<ConsumerRecord<String, V>> batch;
        var nrOfRetries = 0;
        do {
          batch = GeneratorImpl.this.mediator.poll();
        } while (batch.isEmpty() && ++nrOfRetries < GeneratorImpl.this.maxRetries);
        return batch;
      }
    });
  }
}
