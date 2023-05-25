package io.airbyte.integrations.source.kafka.state;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.configoss.StateWrapper;
import io.airbyte.configoss.helpers.StateMessageHelper;
import io.airbyte.protocol.models.v0.AirbyteStateMessage;
import io.airbyte.protocol.models.v0.AirbyteStreamState;
import io.airbyte.protocol.models.v0.StreamDescriptor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.TopicPartition;

public class KafkaStateManager {

  public KafkaStateManager(final JsonNode state) {
    final boolean USE_STREAM_CAPABLE_STATE = true;
    final Optional<StateWrapper> wrapper = StateMessageHelper.getTypedState(state, USE_STREAM_CAPABLE_STATE);
    final var serialisedState = wrapper.map(value ->
        switch (value.getStateType()) {
          case GLOBAL -> fromAirbyteStreamState(value.getGlobal().getGlobal().getStreamStates());
          case STREAM -> fromAirbyteStreamState(value.getStateMessages().stream().map(it -> it.getStream()).toList());
          case LEGACY -> new HashMap<TopicPartition, Long>();
        }
    );

    this._state = serialisedState.orElse(new HashMap<>());
  }

  public List<AirbyteStateMessage> serialise() {
    final Map<String, Map<Integer, Long>> intermediate = new HashMap<>();

    for (final Entry<TopicPartition, Long> entry : this._state.entrySet()) {
      final var topic = entry.getKey().topic();
      final var partition = entry.getKey().partition();
      final var offset = entry.getValue();
      if (!intermediate.containsKey(topic)) {
        intermediate.put(topic, new HashMap<>());
      }
      intermediate.get(topic).put(partition, offset);
    }

    return intermediate
        .entrySet()
        .stream()
        .map(it ->
            new AirbyteStateMessage()
                .withType(AirbyteStateMessage.AirbyteStateType.STREAM)
                .withStream(new AirbyteStreamState()
                    .withStreamDescriptor(new StreamDescriptor().withName(it.getKey()))
                    .withStreamState(Jsons.jsonNode(new KafkaStreamState(it.getValue())))))
        .toList();
  }

  public Map<TopicPartition, Long> state() {
    return SerializationUtils.clone(this._state);
  }

  public void update(final Map<TopicPartition, Long> updates) {
    this._state.putAll(updates);
  }

  private HashMap<TopicPartition, Long> fromAirbyteStreamState(final List<io.airbyte.protocol.models.AirbyteStreamState> states) {
    final var result = new HashMap<TopicPartition, Long>();

    for (final io.airbyte.protocol.models.AirbyteStreamState state : states) {
      final var topic = state.getStreamDescriptor().getName();
      final var stream = Jsons.convertValue(state.getStreamState(), KafkaStreamState.class);

      for (final Entry<Integer, Long> entry : stream.partitions().entrySet()) {
        final var partition = entry.getKey();
        final var offset = entry.getValue();

        result.put(new TopicPartition(topic, partition), offset);
      }
    }

    return result;
  }

  private final HashMap<TopicPartition, Long> _state;
}