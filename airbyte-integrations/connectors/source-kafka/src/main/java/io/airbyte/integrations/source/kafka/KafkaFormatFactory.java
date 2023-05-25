/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.integrations.source.kafka.format.AvroFormat;
import io.airbyte.integrations.source.kafka.format.JsonFormat;
import io.airbyte.integrations.source.kafka.format.KafkaFormat;
import io.airbyte.integrations.source.kafka.state.KafkaStateManager;

public class KafkaFormatFactory {

  public static KafkaFormat getFormat(final JsonNode config, final KafkaStateManager manager) {

    final MessageFormat messageFormat =
        config.has("MessageFormat") ? MessageFormat.valueOf(config.get("MessageFormat").get("deserialization_type").asText().toUpperCase())
            : MessageFormat.JSON;

    switch (messageFormat) {
      case JSON -> {
        return new JsonFormat(config);
      }
      case AVRO -> {
        return new AvroFormat(config, manager);
      }
    }
    return new JsonFormat(config);
  }

}
