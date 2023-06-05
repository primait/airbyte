package io.airbyte.integrations.source.kafka.converter;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.protocol.models.v0.AirbyteRecordMessage;
import io.airbyte.protocol.models.v0.AirbyteMessage;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;


class AvroConverterTest {

    @Test
    void convertToAirbyteRecord() {

        String rawSchema = """
                {
                     "type": "record",
                     "name": "TestRecord",
                     "namespace": "mynamespace",
                     "fields": [{
                         "name": "address",
                         "type": {
                             "type": "record",
                             "name": "Address",
                             "fields": [{
                                 "name": "number",
                                 "type": ["null", "string"],
                                 "default": null
                             }, {
                                 "name": "postal_code",
                                 "type": "int"
                             }, {
                                 "name": "street",
                                 "type": ["null", "string"],
                                 "default": null
                             }]
                         }
                     }, {
                         "name": "name",
                         "type": "string"
                         
                     }, {
                         "name": "skills",
                         "type": ["null", {
                             "type": "array",
                             "items": ["null", "string"]
                         }],
                         "default": null
                     }, {
                         "name": "surname",
                         "type": "string"
                     }]
                 }
                """;
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(rawSchema);

        GenericRecord addressTestRecord = new GenericData.Record(schema.getField("address").schema());
        addressTestRecord.put("street", "via Antonia Pozzi");
        addressTestRecord.put("number", "6");
        addressTestRecord.put("postal_code", 20149);

        List<String> skillsTestRecord = new LinkedList<>();
        skillsTestRecord.add("coding");
        skillsTestRecord.add("etl");


        GenericRecord testRecord = new GenericData.Record(schema);
        testRecord.put("name", "Claudio");
        testRecord.put("surname", "D'Amico");
        testRecord.put("address", addressTestRecord);
        testRecord.put("skills", skillsTestRecord);

        String testTopic = "conversion.uk.test";

        Converter<GenericRecord> converter = new AvroConverter();

        AirbyteMessage actualMessage = converter.convertToAirbyteRecord(testTopic, testRecord);
        AirbyteRecordMessage actualMessageRecord = actualMessage.getRecord();
        JsonNode actualData = actualMessageRecord.getData();

        List<String> expectedSkills = (List<String>) testRecord.get("skills");
        List<String> actualSkills = new ArrayList<>();
        actualData.get("skills").elements().forEachRemaining(x -> actualSkills.add(x.asText()));

        assertAll(
                () -> assertEquals( AirbyteMessage.Type.RECORD, actualMessage.getType()),
                () -> assertEquals(testTopic, actualMessageRecord.getStream()),
                () -> assertEquals(testRecord.get("name"), actualData.get("name").asText()),
                () -> assertEquals(testRecord.get("surname"), actualData.get("surname").asText()),
                () -> assertEquals(expectedSkills.stream().distinct().toList(), actualSkills.stream().distinct().toList()),
                () -> assertEquals(addressTestRecord.get("street"), actualData.get("address").get("street").asText()),
                () -> assertEquals(addressTestRecord.get("number"), actualData.get("address").get("number").asText()),
                () -> assertEquals(addressTestRecord.get("postal_code"), actualData.get("address").get("postal_code").asInt())
        );

    }
}