package io.airbyte.integrations.source.kafka.format;


import static java.util.Map.entry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.protocol.models.Jsons;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Avro2JsonConvert {

  private static final Logger LOGGER = LoggerFactory.getLogger(Avro2JsonConvert.class);
  private static final ObjectMapper mapper = new ObjectMapper();

  /**
   * Mapping from avro to Json type
   *
   * @link https://docs.airbyte.com/understanding-airbyte/json-avro-conversion/#conversion-rules
   */
  private static final Map<String, String> AVRO_TO_JSON_DATA_TYPE_MAPPING = Map.ofEntries(
      entry("null", "null"),
      entry("boolean", "boolean"),
      entry("int", "integer"),
      entry("long", "integer"),
      entry("float", "number"),
      entry("double", "number"),
      entry("bytes", "string"),
      entry("string", "string"),
      entry("record", "object"),
      entry("enum", "string"),
      entry("array", "array"),
      entry("map", "object"),
      entry("fixed", "string")
  );


  /**
   * Method to mapping avro type to json type
   *
   * @param avroType
   * @return
   */
  private String avroTypeToJsonType(final String avroType) {
    final String jsonTypes = AVRO_TO_JSON_DATA_TYPE_MAPPING.get(avroType);
    if (jsonTypes == null) {
      throw new IllegalArgumentException("Unknown Avro type: " + avroType);
    }
    return jsonTypes;
  }

  /**
   * Method to convert the avro schema in to Json schema in order to save the schema in the Airbyte Catalog
   *
   * @param avroSchema
   * @return JsonNode
   * @throws Exception
   */
  public JsonNode convertoToAirbyteJson(final String avroSchema) throws Exception {
    LOGGER.info("Starting to convert Avro schema in Json Schema");
    final JsonNode jsonSchema  = convertoToAirbyteJson( Jsons.deserialize(avroSchema));
    return jsonSchema;
  }


  /**
   * Method to convert the avro schema in to Json schema in order to save the schema in the Airbyte Catalog
   *
   * @param avroSchema JsonNode node with Avro struct
   * @return JsonNode  node Json struct
   * @throws Exception
   * @link https://docs.airbyte.com/understanding-airbyte/json-avro-conversion/
   */
  public JsonNode convertoToAirbyteJson(final JsonNode avroSchema) throws Exception {

    final ObjectNode node = mapper.createObjectNode();
    JsonNode typeFields = null;
    final JsonNode type = removeNull(avroSchema.get("type"));

    if (type.isObject()) {
      return convertoToAirbyteJson(type);
    } else if (type.isValueNode()) {
      typeFields = type;
    } else if (type.isArray() && StreamSupport.stream(type.spliterator(), false).allMatch(t -> t.isTextual())) {
      final ArrayNode array = node.putArray("anyOf");
      for (final Iterator<JsonNode> it = type.iterator(); it.hasNext(); ) {
        final JsonNode typeIt = it.next();
        array.add(mapper.createObjectNode().put("type", avroTypeToJsonType(typeIt.asText())));
      }
      return node;
    }
    if (typeFields == null) {
      StreamSupport.stream(avroSchema.get("type").spliterator(), false).filter(t -> !t.isNull()).filter(t -> !t.asText().equals("null"))
          .forEach(t -> node.put("type", avroTypeToJsonType(t.asText())));
      return node;

    }
    final String typeT = typeFields.asText();
    switch (typeT) {
      case "record" -> {
        node.put("type", "object");
        final ObjectNode properties = mapper.createObjectNode();
        for (final Iterator<JsonNode> it = avroSchema.get("fields").iterator(); it.hasNext(); ) {
          final JsonNode field = it.next();
          properties.put(field.get("name").asText(), convertoToAirbyteJson(field));
        }
        node.set("properties", properties);
        return node;
      }
      case "string", "int", "null", "float", "boolean" -> {
        return node.put("type", avroTypeToJsonType(typeT));
      }
      case "map" -> {
        final JsonNode typeObj = mapper.createObjectNode().put("type", "string");
        final JsonNode typeObj1 = mapper.createObjectNode()
            .put("type", "object")
            .set("additionalProperties", typeObj);
        node.set("type", typeObj1);
        return node;
      }
      case "array" -> {
        final ArrayNode array = node.putArray("items");
        node.put("type", "array");
        final JsonNode items = removeNull(avroSchema.get("items"));

        if (items.isValueNode()) {
          array.add(mapper.createObjectNode().put("type", avroTypeToJsonType(items.asText())));
        } else {
          final JsonNode a = convertoToAirbyteJson(items);
          array.add(a);
        }
        return node;
      }
    }
    return node;
  }


  private static Object removeNull(final List field) throws Exception {
    final Optional<Object> fieldWithoutNull = field.stream().filter(x -> (x != null) && (!x.equals("null"))).findFirst();
    if (fieldWithoutNull.isEmpty()) {
      throw new Exception("Unknown Avro converter:" + field);
    }
    return fieldWithoutNull.get();
  }

  /**
   * Remove null or "null" value present in the Type array
   *
   * @param field
   * @return
   * @throws Exception
   */
  private static JsonNode removeNull(final JsonNode field) throws Exception {
    ArrayNode array = null;
    if (field.isTextual()) {
      return field;
    } else if (field.isObject()) {
      array = mapper.createArrayNode().add(field).add(mapper.createObjectNode().textNode("null"));
    } else if (field.isArray()) {
      array = (ArrayNode) field; //devono essere tutti value node
    }

    final List<JsonNode> fieldWithoutNull = StreamSupport.stream(array.spliterator(), false)
        .filter(t -> !t.isNull()).filter(t -> !t.asText().equals("null")).toList();
    if (fieldWithoutNull.isEmpty()) {
      throw new Exception("Unknown JsonNode converter:" + field);
    } else {
      if (fieldWithoutNull.size() == 1) {
        return fieldWithoutNull.stream().findFirst().get();
      } else {

        final ArrayNode arrayNode = mapper.createArrayNode();
        fieldWithoutNull.stream().forEach(arrayNode::add);
        return (JsonNode) arrayNode;
      }
    }
  }


}