package io.airbyte.integrations.source.kafka.format;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.JsonObject;
import io.airbyte.protocol.models.Jsons;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Map.entry;

public class Avro2JsonConvert {

  private static final Logger LOGGER = LoggerFactory.getLogger(Avro2JsonConvert.class);
  private final ObjectMapper mapper = new ObjectMapper();

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
    final Map<String, Object> mapAvroSchema = mapper.readValue(avroSchema, new TypeReference<>() {
    });
    final Map<String, Object> mapJsonSchema = convertoToAirbyteJson(mapAvroSchema);
    final JsonNode jsonSchema = Jsons.deserialize(mapper.writeValueAsString(mapJsonSchema));
    return jsonSchema;
  }

  /**
   * Method to convert the avro schema in to Json schema in order to save the schema in the Airbyte Catalog
   *
   * @param avroSchema Map<String, Object> map with Avro struct
   * @return Map<String, Object> map with Json struct
   * @throws Exception
   * @link https://docs.airbyte.com/understanding-airbyte/json-avro-conversion/
   */
  public Map<String, Object> convertoToAirbyteJson(final Map<String, Object> avroSchema) throws Exception {
    final Map<String, Object> jsonSchema = new HashMap<>();
    final List<Map<String, Object>> fields = (List<Map<String, Object>>) avroSchema.get("fields");
    for (final Map<String, Object> field : fields) {
      final String fieldName = (String) field.get("name");
      Object fieldSchema = null;
      List<Object> filedTypes = null;
      if (field.get("type") instanceof List) {
        final List fieldType = (List<Object>) field.get("type");
        filedTypes = fieldType.stream().filter(x -> (x != null) && (!x.equals("null"))).toList();
        //Case when there is a list of type ex. ["null", "string"]
        if (filedTypes instanceof List && filedTypes.stream().filter(x -> x instanceof String).count() >= 1) {
          if (filedTypes.stream().filter(x -> x instanceof String).count() == 1) {
            final String jsonType = filedTypes.stream().findFirst()
                .map(t -> avroTypeToJsonType((String) t)).get();
            fieldSchema = Map.of("type", jsonType);

          } else if (filedTypes.stream().filter(x -> x instanceof String).count() > 1) {

            final List<Object> anyOfSchemas = new ArrayList<>();
            fieldType.forEach(type -> anyOfSchemas.add(Map.of("type", avroTypeToJsonType((String) type))));
            fieldSchema = Map.of("anyOf", anyOfSchemas);
          }
        } else {
          final Map<String, Object> mapType = (Map<String, Object>) removeNull(fieldType);
          if (mapType.get("type").equals("array") && mapType.get("items") instanceof List) {
            final List<Object> typeList = (ArrayList<Object>) mapType.get("items");
            final Object items = removeNull(typeList);
            if (items instanceof Map) {
              //Case when there is a List of Object
              fieldSchema = Map.of("type", avroTypeToJsonType("array"), "items", List.of(convertoToAirbyteJson((Map<String, Object>) items)));
            } else {
              //Case when there is a List of type
              final List<Map<String, String>> types = typeList
                  .stream()
                  .map(x -> (String) x)
                  .map(x -> Map.of("type", avroTypeToJsonType(x))).toList();
              fieldSchema = Map.of("type", avroTypeToJsonType("array"), "items", types);
            }
          } else if (mapType.get("type").equals("array") && mapType.get("items") instanceof Map) {
            //Case when there is a single Object
            fieldSchema = Map.of("type", avroTypeToJsonType("array"), "items", convertoToAirbyteJson((Map<String, Object>) mapType.get("items")));
          } else {
            fieldSchema = convertoToAirbyteJson(mapType);
          }

        }
      } else if (field.get("type") instanceof Map && ((Map<String, Object>) field.get("type")).get("type").equals("map")) {
        //Case when there are a list of Object not in the array
        final Object fieldType = ((Map<String, Object>) ((Map<String, Object>) field.get("type")).get("values")).get("type");
        final Map<String, Object> map3 = Map.of("type", "map", "values", avroTypeToJsonType((String) fieldType));

        fieldSchema = map3;
      } else if (field.get("type") instanceof Map) {
        //Case when there are a list of Object not in the array
        final Map<String, Object> fieldType = (Map<String, Object>) field.get("type");
        // Map<String, Object> map3 = Stream.of(Map.of("type", new String[]{"object", "null"}), convertoToAirbyteJson(fieldType))
        final Map<String, Object> map3 = Stream.of(convertoToAirbyteJson(fieldType))
            .flatMap(map -> map.entrySet().stream())
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue));
        fieldSchema = map3;
      } else if (field.get("type") instanceof List) {
        final List<String> fieldTypes = (List<String>) field.get("type");
        final List<Object> anyOfSchemas = new ArrayList<>();
        fieldTypes.forEach(type -> anyOfSchemas.add(avroTypeToJsonType(type)));
        for (final String type : fieldTypes) {
          if (!type.equals("fields")) {
            continue;
          }
          anyOfSchemas.add(avroTypeToJsonType(type));
        }
        fieldSchema = Map.of("anyOf", anyOfSchemas);
      } else {
        final String singleType = List.of((String) field.get("type")).stream()
            .filter(type -> !"null".equals(type))
            .findFirst()
            .orElse(null);
        fieldSchema = Map.of("type", avroTypeToJsonType(singleType));
      }
      jsonSchema.put(fieldName, fieldSchema);
    }
    return Map.of("type", avroTypeToJsonType("map"), "properties", jsonSchema);
  }

  public JsonNode convertoToAirbyteJson2(final JsonNode avroSchema) {

//pulire da eventuali null

    // final String name = avroSchema.get("name").asText();
    final ObjectNode node = mapper.createObjectNode();
//    if (avroSchema.isArray()){
//      final ArrayNode array = node.putArray("type");
//      StreamSupport.stream(avroSchema.spliterator(), false)
//          .forEach(t -> {
//            if(t.isTextual()) {
//              array.add(avroTypeToJsonType(t.asText()));
//            }
//            else
//              array.add(convertoToAirbyteJson2(t));
//
//          });
//      return node;
//    }

//    else if(avroSchema.get("type").isArray() && StreamSupport.stream(avroSchema.get("type").spliterator(), false)  .filter(t -> !t.asText().equals("null")).allMatch(t -> t.isTextual())) {
//      final ArrayNode array = node.putArray("type");
//      StreamSupport.stream(avroSchema.get("type").spliterator(), false)
//          .filter(t -> !t.asText().equals("null"))
//          .map(t -> avroTypeToJsonType(t.asText())).forEach(t -> array.add(t));
//      return node;
//
//    }
    //   else
    final JsonNode typeFields;
    if (avroSchema.get("type").isArray()) {
      typeFields = StreamSupport.stream(avroSchema.get("type").spliterator(), false).filter(t -> !t.isNull()).filter(t -> !t.asText().equals("null"))
          .toList().get(0).get("type");
    } else if (avroSchema.get("type").isObject() && avroSchema.get("type").get("type").asText().equals("array")) {
      typeFields = avroSchema.get("type").get("type");
    } else if (avroSchema.get("type").isObject() && avroSchema.get("type").get("type").asText().equals("map")) {
      typeFields = avroSchema.get("type").get("type");
    }
      else {
        typeFields = avroSchema.get("type");
      }

    if (typeFields==null){
     StreamSupport.stream(avroSchema.get("type").spliterator(), false).filter(t -> !t.isNull()).filter(t -> !t.asText().equals("null"))
          .forEach(t ->  node.put("type", avroTypeToJsonType(t.asText())));
     return node;

    }
    // if(avroSchema.get("type").isArray() ){
    //final List<JsonNode> fields = StreamSupport.stream(avroSchema.get("type").spliterator(), false).filter(t -> !t.isNull()).filter(t -> !t.asText().equals("null")).toList();
//    if (typeFields.isTextual()) {
//
//      node.put("type", avroTypeToJsonType(typeFields.asText()));
//    } else
    //  if (typeFields.asText().equals("array")) {
      //node.put("nzmd", "array");

 //  }
//    else if (typeFields.asText().equals("record")) {
//      final JsonNode a = convertoToAirbyteJson2(avroSchema.get("type"));
//
//      return a;
//    }

    //}
//    else if (avroSchema.get("type").isObject() && avroSchema.get("type").get("type").asText().equals("map")) {
//      final JsonNode a = convertoToAirbyteJson2(avroSchema.get("type").get("values"));
//      node.put("type", "map");
//      node.putArray("values").add(a);
//      return node;
//    }
//    else if (avroSchema.get("type").isObject() && avroSchema.get("type").get("type").asText().equals("map")) {
//      final JsonNode a = convertoToAirbyteJson2(avroSchema.get("type").get("values"));
//      node.put("type", "map");
//      node.putArray("values").add(a);
//      return node;
//    }
//    else if (avroSchema.get("type").isObject() && avroSchema.get("type").get("type").asText().equals("record")) {
//      final JsonNode a = convertoToAirbyteJson2(avroSchema.get("type"));
//
//      return a;
 //   }
//    else if (avroSchema.get("type").isArray() && !StreamSupport.stream(avroSchema.get("type").spliterator(), false).allMatch(t -> t.isArray())) {
//      final ArrayNode array = node.putArray("type");
//      StreamSupport.stream(avroSchema.get("type").spliterator(), false)
//          .filter(t -> !t.isNull()).filter(t -> !t.asText().equals("null"))
//
//          .forEach(t -> {
//            if (t.isTextual()) {
//              array.add(avroTypeToJsonType(t.asText()));
//            } else {
//              final JsonNode a = convertoToAirbyteJson2(t);
//              array.add(a);
//            }
//
//          });
//      //convertoToAirbyteJson2()
//      return node;

  //  } else
      if (typeFields.isTextual()) {
        final String type;
        if (avroSchema.get("type").isTextual())
          type = avroSchema.get("type").asText();
        else
          type = StreamSupport.stream(avroSchema.get("type").spliterator(), false)
              .filter(t -> !t.isNull())
             .filter(t -> t.isObject() && !t.asText().equals("null")).toList()
       .get(0).get("type").asText();

      switch (type) {
        case "record" -> {
          node.put("type", "object");
          final ObjectNode properties = mapper.createObjectNode();
          final JsonNode items;
          if (avroSchema.get("type").isTextual())
            items = avroSchema.get("fields");
          else if (avroSchema.get("type").isObject() && avroSchema.get("type").get("type").asText().equals("array")) {
            items = avroSchema.get("type").get("items").get("fields");
          }
            else {
            items = StreamSupport.stream(avroSchema.get("type").spliterator(), false)
                .filter(t -> !t.isNull() || t.isTextual() && !t.asText().equals("null"))
                .toList()
                .get(0).get("fields");
          }

          final List<JsonNode> a = StreamSupport.stream(avroSchema.get("type").spliterator(), false)
              .filter(t -> !t.isNull() || (t.isTextual() && !t.asText().equals("null")))
              .toList();

          StreamSupport.stream(items.spliterator(), false)
              .forEach(field -> properties.put(field.get("name").asText(), convertoToAirbyteJson2(field)));
          node.set("properties", properties);
          return node;
        }
        case "string", "int", "null", "float", "boolean" -> {
          return node.put("type", avroTypeToJsonType(type));
        }
        case "map" -> {
          final JsonNode a = node.put(avroSchema.get("name").asText(),
              mapper.createObjectNode()
                  .put("type",
                      mapper.createObjectNode()
                          .put("type", "object")
                          .put("additionalProperties",
                              mapper.createObjectNode()
                                  .put("type", "string"))));

          return a;
        }
        case "array" -> {
          final ArrayNode array = node.putArray("items");
          node.put("type", "array");
          //array.add(convertoToAirbyteJson2(fields.get(0).get("items")));
          StreamSupport.stream(StreamSupport.stream(avroSchema.get("type").spliterator(), false).filter(t -> !t.isNull()).filter(t -> !t.asText().equals("null")).toList().get(0)
                  .get("items").spliterator(), false)
              .filter(t -> !t.isNull()).filter(t -> !t.asText().equals("null"))
              .forEach(t -> {
                if (t.isTextual()) {
                  array.add(mapper.createObjectNode().put("type", avroTypeToJsonType(t.asText())));
                } else {
                  final JsonNode a = convertoToAirbyteJson2(t);
                  array.add(a);
                }

              });
          return node;
        }
      }
    }

    return node;
//    final List<Map<String, Object>> fields = (List<Map<String, Object>>) avroSchema.get("fields");
//    for (final Map<String, Object> field : fields) {
//      final String fieldName = (String) field.get("name");
//      Object fieldSchema = null;
//      List<Object> filedTypes = null;
//      if (field.get("type") instanceof List) {
//        final List fieldType = (List<Object>) field.get("type");
//        filedTypes = fieldType.stream().filter(x -> (x != null) && (!x.equals("null"))).toList();
//        //Case when there is a list of type ex. ["null", "string"]
//        if (filedTypes instanceof List && filedTypes.stream().filter(x -> x instanceof String).count() > 0) {
//          if (filedTypes.stream().filter(x -> x instanceof String).count() == 1) {
//            final List<String> jsonTypes = fieldType.stream()
//                .map(t -> avroTypeToJsonType((String)t))
//                .toList();
//            fieldSchema = Map.of("type", jsonTypes);
//
//          } else if (filedTypes.stream().filter(x -> x instanceof String).count() > 1) {
//
//            final List<Object> anyOfSchemas = new ArrayList<>();
//            fieldType.forEach(type -> anyOfSchemas.add(Map.of("type", avroTypeToJsonType((String) type))));
//            fieldSchema = Map.of("anyOf", anyOfSchemas);
//          }
//        } else {
//          final Map<String, Object> mapType = (Map<String, Object>) removeNull(fieldType);
//          if (mapType.get("type").equals("array") && mapType.get("items") instanceof List) {
//            final List<Object> typeList = (ArrayList<Object>) mapType.get("items");
//            final Object items = removeNull(typeList);
//            if (items instanceof Map) {
//              //Case when there is a List of Object
//              fieldSchema = Map.of("type", avroTypeToJsonType("array"), "items", convertoToAirbyteJson((Map<String, Object>) items));
//            } else {
//              //Case when there is a List of type
//              final List<Map<String, String>> types = typeList
//                  .stream()
//                  .map(x -> (String)x)
//                  .map(x -> Map.of("type", avroTypeToJsonType(x))).toList();
//              fieldSchema = Map.of("type", avroTypeToJsonType("array"), "items", types);
//            }
//          } else if (mapType.get("type").equals("array") && mapType.get("items") instanceof Map) {
//            //Case when there is a single Object
//            fieldSchema = Map.of("type", avroTypeToJsonType("array"), "items", convertoToAirbyteJson((Map<String, Object>) mapType.get("items")));
//          } else {
//            fieldSchema = convertoToAirbyteJson(mapType);
//          }
//
//        }
//      } else if (field.get("type") instanceof Map) {
//        //Case when there are a list of Object not in the array
//        final Map<String, Object> fieldType = (Map<String, Object>) field.get("type");
//        // Map<String, Object> map3 = Stream.of(Map.of("type", new String[]{"object", "null"}), convertoToAirbyteJson(fieldType))
//        final Map<String, Object> map3 = Stream.of(convertoToAirbyteJson(fieldType))
//            .flatMap(map -> map.entrySet().stream())
//            .collect(Collectors.toMap(
//                Map.Entry::getKey,
//                Map.Entry::getValue));
//        fieldSchema = map3;
//      } else if (field.get("type") instanceof List) {
//        final List<String> fieldTypes = (List<String>) field.get("type");
//        final List<Object> anyOfSchemas = new ArrayList<>();
//        fieldTypes.forEach(type -> anyOfSchemas.add(avroTypeToJsonType(type)));
//        for (final String type : fieldTypes) {
//          if (!type.equals("fields")) {
//            continue;
//          }
//          anyOfSchemas.add(avroTypeToJsonType(type));
//        }
//        fieldSchema = Map.of("anyOf", anyOfSchemas);
//      } else {
//        final String singleType = List.of((String) field.get("type")).stream()
//            .filter(type -> !"null".equals(type))
//            .findFirst()
//            .orElse(null);
//        fieldSchema = Map.of("type", avroTypeToJsonType(singleType));
//      }
//      jsonSchema.put(fieldName, fieldSchema);
//    }
//    return Map.of("type", avroTypeToJsonType("map"), "properties", jsonSchema);
  }

  /**
   * Remove null or "null" value present in the Type array
   *
   * @param field
   * @return
   * @throws Exception
   */
  private static Object removeNull(final List field) throws Exception {
    final Optional<Object> fieldWithoutNull = field.stream().filter(x -> (x != null) && (!x.equals("null"))).findFirst();
    if (fieldWithoutNull.isEmpty()) {
      throw new Exception("Unknown Avro converter:" + field);
    }
    return fieldWithoutNull.get();
  }


}