/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.integrations.source.kafka.format.Avro2JsonConvert;
import io.airbyte.protocol.models.Jsons;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AvroConverterTest {

  ObjectMapper mapper = new ObjectMapper();

//  String avroSimpleSchema = """
//      {
//          "type": "record",
//          "name": "sampleAvro",
//          "namespace": "AVRO",
//          "fields": [
//              {"name": "name", "type": "string"},
//              {"name": "age", "type": ["int", "null"]},
//              {"name": "address", "type": ["float", "null"]},
//              {"name": "street", "type": "float"},
//              {"name": "valid", "type": "boolean"}
//          ]
//      }
//          """;
//
//  String jsonSimpleSchema = """
//      {
//       "type": "object",
//       "properties": {
//            "address": {"type": ["number", "null"]},
//            "age": {"type": ["integer", "null"]},
//            "name": {"type": "string"},
//            "street": {"type": "number"},
//            "valid": {"type": "boolean"}
//        }
//      }
//
//       """;


//  String avroNestedRecordsSchema = """
//        {
//          "type": "record",
//          "name": "sampleAvroNested",
//          "namespace": "AVRO",
//          "fields": [
//              {"name": "lastname", "type": "string"},
//              {"name": "address","type": {
//                              "type" : "record",
//                              "name" : "AddressUSRecord",
//                              "fields" : [
//                                  {"name": "streetaddress", "type": ["string", "null"]},
//                                  {"name": "city", "type": "string"}
//                              ]
//                          }
//              }
//          ]
//      }
//                """;
//
//
//  String jsonNestedRecordSchema = """
//       {
//       "type": "object",
//       "properties": {
//              "address":{
//              "type": "object",
//               "properties": {
//                         "city":{
//                              "type": "string"
//                               },
//                         "streetaddress":{
//                              "type":["string","null"]
//                         }
//                 }
//              },
//              "lastname":{
//                  "type":"string"
//              }
//          }
//       }
//      """;
//
//
//  String avroWithArraySchema = """
//      {
//        "type": "record",
//        "fields": [
//          {
//            "name": "identifier",
//            "type": [
//              null,
//              {
//                "type": "array",
//                "items": ["null", "string"]
//              }
//            ]
//          }
//        ]
//      }
//
//      """;
//
//  String jsonWithArraySchema = """
//      {
//       "type": "object",
//       "properties": {
//              "identifier": {
//                    "type": "array",
//                    "items" :  [
//                      {"type":["null"]},
//                      {"type":["string"]}
//                    ]
//                  }
//              }
//          }
//      """;
//
//  String avroWithArrayAndRecordSchema = """
//      {
//          "type": "record",
//          "name": "TestObject",
//          "namespace": "ca.dataedu",
//          "fields": [{
//              "name": "array_field",
//              "type": ["null", {
//                  "type": "array",
//                  "items": ["null", {
//                      "type": "record",
//                      "name": "Array_field",
//                      "fields": [{
//                          "name": "id",
//                          "type": ["null", {
//                              "type": "record",
//                              "name": "Id",
//                              "fields": [{
//                                  "name": "id_part_1",
//                                  "type": ["null", "int"],
//                                  "default": null
//                              }]
//                          }],
//                          "default": null
//                      }]
//                  }]
//              }],
//              "default": null
//          }]
//      }
//
//      """;
//
//
//  String jsonWithArrayAndRecordSchema = """
//      {
//      	"type":[
//      		"object",
//      		"null"
//      	],
//      	"properties":{
//      		"array_field":{
//      			"type":[
//      				"array",
//      				"null"
//      			],
//      			"items":[
//      				{
//      					"type":[
//      						"object",
//      						"null"
//      					],
//                          "properties":{
//                              "id":{
//                                  "type":[
//                                      "object",
//                                      "null"
//                                  ],
//                                  "properties":{
//                                      "id_part_1":{
//                                          "type":[
//                                              "integer",
//                                              "null"
//                                          ]
//                                      }
//                                  }
//                              }
//                          }
//
//      				}
//      			]
//      		}
//      	}
//      }
//      """;
//
//
//  String avroWithArrayAndNestedRecordSchema = """
//      {
//          "type": "record",
//          "name": "TestObject",
//          "namespace": "ca.dataedu",
//          "fields": [{
//              "name": "array_field",
//              "type": ["null", {
//                  "type": "array",
//                  "items": ["null", {
//                      "type": "record",
//                      "name": "Array_field",
//                      "fields": [{
//                          "name": "id",
//                          "type": ["null", {
//                              "type": "record",
//                              "name": "Id",
//                              "fields": [{
//                                  "name": "id_part_1",
//                                  "type": ["null", "int"],
//                                  "default": null
//                              }, {
//                                  "name": "id_part_2",
//                                  "type": ["null", "string"],
//                                  "default": null
//                              }]
//                          }],
//                          "default": null
//                      }, {
//                          "name": "message",
//                          "type": ["null", "string"],
//                          "default": null
//                      }]
//                  }]
//              }],
//              "default": null
//          }]
//      }
//
//      """;
//
//  String jsonWithArrayAndNestedRecordSchema = """
//      {
//          "type": "object",
//          "properties":{
//            "array_field": {
//              "type": ["array", "null"],
//              "items": [
//                {  "type":"object",
//                   "properties":{
//                        "id": {
//
//                             "type":"object",
//                              "properties":{
//                                "id_part_1": { "type": ["integer", "null"] },
//                                "id_part_2": { "type": ["string", "null"] }
//                            }
//                        },
//                        "message" : {"type": [ "string", "null"] }
//                     }
//                }
//              ]
//            }
//         }
//      }
//      """;
//
//
//  String avroWithCombinedRestrictionsSchema = """
//          {
//          "type": "record",
//          "name": "sampleAvro",
//          "namespace": "AVRO",
//          "fields": [
//              {"name": "name", "type": "string"},
//              {"name": "age", "type": ["int", "null"]},
//              {"name": "address", "type": ["float", "string", "null"]}
//          ]
//      }
//          """;
//
//  String jsonWithCombinedRestrictionsSchema = """
//      {
//          "type":"object",
//          "properties":{
//               "address": {"anyOf": [
//                              {"type": "number"},
//                              {"type": "string"},
//                              {"type": "null"}
//                           ]},
//                "name": {"type": "string"},
//                "age": {"type": ["integer", "null"]}
//          }
//      }
//
//       """;


  @Test
  public void testConverterAvroSimpleSchema() throws Exception {

    final String avroSimpleSchema = getFileFromResourceAsString("/converter/simpleSchema.avsc");
    final String jsonSimpleSchema = getFileFromResourceAsString("/converter/simpleSchema.json");

    final Avro2JsonConvert converter = new Avro2JsonConvert();
    final JsonNode actual  = converter.convertoToAirbyteJson2( Jsons.deserialize(avroSimpleSchema));
    final JsonNode expect = mapper.readTree(jsonSimpleSchema);
    //final JsonNode actual = mapper.readValue(mapper.writeValueAsString(airbyteSchema), JsonNode.class);
    assertEquals(expect, actual);
  }

  @Test
  public void testConverterAvroNestedRecordsSchema() throws Exception {

    final String avroNestedRecordsSchema = getFileFromResourceAsString("/converter/nestedRecordsSchema.avsc");
    final String jsonNestedRecordSchema = getFileFromResourceAsString("/converter/nestedRecordsSchema.json");
    final Avro2JsonConvert converter = new Avro2JsonConvert();
    final JsonNode actual  = converter.convertoToAirbyteJson2( Jsons.deserialize((avroNestedRecordsSchema)));
    final JsonNode expect = mapper.readTree(jsonNestedRecordSchema);
    //final JsonNode actual = mapper.readValue(mapper.writeValueAsString(airbyteSchema), JsonNode.class);
    assertEquals(expect, actual);
  }

  @Test
  public void testConverterAvroWithArray() throws Exception {

    final String avroWithArraySchema = getFileFromResourceAsString("/converter/withArraySchema.avsc");
    final String jsonWithArraySchema = getFileFromResourceAsString("/converter/withArraySchema.json");

    final Avro2JsonConvert converter = new Avro2JsonConvert();
    final JsonNode actual = converter.convertoToAirbyteJson2( Jsons.deserialize(avroWithArraySchema));
    final JsonNode expect = mapper.readTree(jsonWithArraySchema);
    //final JsonNode actual = mapper.readValue(mapper.writeValueAsString(airbyteSchema), JsonNode.class);
    assertEquals(expect, actual);
  }


  @Test
  public void testConverterAvroWithArrayAndRecordSchema() throws Exception {

    final String avroWithArrayAndRecordSchema = getFileFromResourceAsString("/converter/withArrayAndRecordSchema.avsc");
    final String jsonWithArrayAndRecordSchema = getFileFromResourceAsString("/converter/withArrayAndRecordSchema.json");


    final Avro2JsonConvert converter = new Avro2JsonConvert();
    final JsonNode actual = converter.convertoToAirbyteJson2( Jsons.deserialize(avroWithArrayAndRecordSchema));
    final JsonNode expect = mapper.readTree(jsonWithArrayAndRecordSchema);
    //final JsonNode actual = mapper.readValue(mapper.writeValueAsString(airbyteSchema), JsonNode.class);
    assertEquals(expect, actual);
  }


  @Test
  public void testConverterAvroWithCombinedRestrictions() throws Exception {

    final String avroWithCombinedRestrictionsSchema = getFileFromResourceAsString("/converter/withCombinedRestrictionsSchema.avsc");
    final String jsonWithCombinedRestrictionsSchema = getFileFromResourceAsString("/converter/withCombinedRestrictionsSchema.json");

    final Map<String, Object> jsonSchema = mapper.readValue(avroWithCombinedRestrictionsSchema, new TypeReference<>() {
    });
    final Avro2JsonConvert converter = new Avro2JsonConvert();
    final Map<String, Object> airbyteSchema = converter.convertoToAirbyteJson(jsonSchema);
    final JsonNode expect = mapper.readTree(jsonWithCombinedRestrictionsSchema);
    final JsonNode actual = mapper.readValue(mapper.writeValueAsString(airbyteSchema), JsonNode.class);
    assertEquals(expect, actual);
  }


  @Test
  public void testConverterAvroWithArrayAndNestedRecordSchema() throws Exception {

    final String avroWithArrayAndNestedRecordSchema = getFileFromResourceAsString("/converter/withArrayAndNestedRecordSchema.avsc");
    final String jsonWithArrayAndNestedRecordSchema = getFileFromResourceAsString("/converter/withArrayAndNestedRecordSchema.json");

    final Map<String, Object> jsonSchema = mapper.readValue(avroWithArrayAndNestedRecordSchema, new TypeReference<>() {
    });
    final Avro2JsonConvert converter = new Avro2JsonConvert();
    final Map<String, Object> airbyteSchema = converter.convertoToAirbyteJson(jsonSchema);
    final JsonNode expect = mapper.readTree(jsonWithArrayAndNestedRecordSchema);
    final JsonNode actual = mapper.readValue(mapper.writeValueAsString(airbyteSchema), JsonNode.class);
    assertEquals(expect, actual);
  }

  @Test
  public void testConverterAvroWithSchemaReference() throws Exception {

    final String avroWithSchemaReference = getFileFromResourceAsString("/converter/withSchemaReference.avsc");
    final String jsonWithSchemaReference = getFileFromResourceAsString("/converter/withSchemaReference.json");

    final Map<String, Object> jsonSchema = mapper.readValue(avroWithSchemaReference, new TypeReference<>() {
    });
    final Avro2JsonConvert converter = new Avro2JsonConvert();
    final JsonNode actual = converter.convertoToAirbyteJson2( Jsons.deserialize(avroWithSchemaReference));
    final JsonNode expect = mapper.readTree(jsonWithSchemaReference);
    final String a = actual.toPrettyString();
    System.out.println(a);
    //final JsonNode actual = mapper.readValue(mapper.writeValueAsString(airbyteSchema), JsonNode.class);
    assertEquals(expect, actual);
  }


  @Test
  public void testConvertoToAirbyteJson() throws Exception {
    final String avroSimpleSchema = getFileFromResourceAsString("/converter/simpleSchema.avsc");
    final String jsonSimpleSchema = getFileFromResourceAsString("/converter/simpleSchema.json");
    final Avro2JsonConvert converter = new Avro2JsonConvert();
    final JsonNode actual = converter.convertoToAirbyteJson(avroSimpleSchema);
    final JsonNode expect = mapper.readTree(jsonSimpleSchema);
    assertEquals(expect, actual);
  }


  private String getFileFromResourceAsString(final String fileName) throws IOException {

    // The class loader that loaded the class
    final InputStream inputStream = getClass().getResourceAsStream(fileName);
    return IOUtils.toString(inputStream, Charset.defaultCharset());

  }


//  @Test
//  public void testConverterAvroSimpleSchema2() throws Exception {
//
//    final String avroSimpleSchema = getFileFromResourceAsString("/converter/simpleSchema.avsc");
//    final String jsonSimpleSchema = getFileFromResourceAsString("/converter/simpleSchema.json");
//    final Map<String, Object> jsonSchema = mapper.readValue(avroSimpleSchema, new TypeReference<>() {
//    });
//    final Avro2JsonConvert converter = new Avro2JsonConvert();
//    final JsonNode airbyteSchema = converter.convertoToAirbyteJson2( Jsons.deserialize(avroSimpleSchema));
//    final JsonNode expect = mapper.readTree(jsonSimpleSchema);
//    final JsonNode actual = mapper.readValue(mapper.writeValueAsString(airbyteSchema), JsonNode.class);
//
//    assertEquals(expect, actual);
//  }

//  @Test
//  public void testConverterAvroWithArrayAndRecordSchema2() throws Exception {
//
//    final String avroWithArrayAndRecordSchema = getFileFromResourceAsString("/converter/withArrayAndRecordSchema.avsc");
//    final String jsonWithArrayAndRecordSchema = getFileFromResourceAsString("/converter/withArrayAndRecordSchema.json");
//
//
//    final Avro2JsonConvert converter = new Avro2JsonConvert();
//    final JsonNode airbyteSchema = converter.convertoToAirbyteJson2(Jsons.deserialize(avroWithArrayAndRecordSchema));
//    final JsonNode expect = mapper.readTree(jsonWithArrayAndRecordSchema);
//    final JsonNode actual = mapper.readValue(mapper.writeValueAsString(airbyteSchema), JsonNode.class);
//    System.out.println(expect);
//    System.out.println(actual);
//    assertEquals(expect, actual);
//  }


}
