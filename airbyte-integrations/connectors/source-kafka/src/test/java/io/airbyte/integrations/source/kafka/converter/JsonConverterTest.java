package io.airbyte.integrations.source.kafka.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.protocol.models.v0.AirbyteMessage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class JsonConverterTest {

    @Test
    void testConvertToAirbyteRecord() throws JsonProcessingException {
        String recordString = """
                {
                   "name": "Claudio",
                   "surname": "D'Amico",
                   "age": 24
                }
                """;

        ObjectMapper mapper = new ObjectMapper();
        JsonNode testRecord = mapper.readTree(recordString);

        String testTopic = "test_topic";

        Converter<JsonNode> converter = new JsonConverter();

        AirbyteMessage actualMessage = converter.convertToAirbyteRecord(testTopic, testRecord);

        assertAll(
                () -> assertEquals(AirbyteMessage.Type.RECORD, actualMessage.getType()),
                () -> assertEquals(testTopic, actualMessage.getRecord().getStream()),
                () -> assertEquals(testRecord, actualMessage.getRecord().getData())
        );


    }
}