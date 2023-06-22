/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.integrations.source.kafka.converter.schema.Avro2JsonConvert;
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



  @Test
  public void testConverterAvroSimpleSchema() throws Exception {

    final String avroSimpleSchema = getFileFromResourceAsString("/converter/simpleSchema.avsc");
    final String jsonSimpleSchema = getFileFromResourceAsString("/converter/simpleSchema.json");

    final Avro2JsonConvert converter = new Avro2JsonConvert();
    final JsonNode actual  = converter.convertoToAirbyteJson( Jsons.deserialize(avroSimpleSchema));
    final JsonNode expect = mapper.readTree(jsonSimpleSchema);
    assertEquals(expect, actual);
  }

  @Test
  public void testConverterAvroNestedRecordsSchema() throws Exception {

    final String avroNestedRecordsSchema = getFileFromResourceAsString("/converter/nestedRecordsSchema.avsc");
    final String jsonNestedRecordSchema = getFileFromResourceAsString("/converter/nestedRecordsSchema.json");
    final Avro2JsonConvert converter = new Avro2JsonConvert();
    final JsonNode actual  = converter.convertoToAirbyteJson( Jsons.deserialize((avroNestedRecordsSchema)));
    final JsonNode expect = mapper.readTree(jsonNestedRecordSchema);
    assertEquals(expect, actual);
  }

  @Test
  public void testConverterAvroWithArray() throws Exception {

    final String avroWithArraySchema = getFileFromResourceAsString("/converter/withArraySchema.avsc");
    final String jsonWithArraySchema = getFileFromResourceAsString("/converter/withArraySchema.json");

    final Avro2JsonConvert converter = new Avro2JsonConvert();
    final JsonNode actual = converter.convertoToAirbyteJson( Jsons.deserialize(avroWithArraySchema));
    final JsonNode expect = mapper.readTree(jsonWithArraySchema);
    assertEquals(expect, actual);
  }


  @Test
  public void testConverterAvroWithArrayAndRecordSchema() throws Exception {

    final String avroWithArrayAndRecordSchema = getFileFromResourceAsString("/converter/withArrayAndRecordSchema.avsc");
    final String jsonWithArrayAndRecordSchema = getFileFromResourceAsString("/converter/withArrayAndRecordSchema.json");


    final Avro2JsonConvert converter = new Avro2JsonConvert();
    final JsonNode actual = converter.convertoToAirbyteJson( Jsons.deserialize(avroWithArrayAndRecordSchema));
    final JsonNode expect = mapper.readTree(jsonWithArrayAndRecordSchema);
    assertEquals(expect, actual);
  }


  @Test
  public void testConverterAvroWithCombinedRestrictions() throws Exception {

    final String avroWithCombinedRestrictionsSchema = getFileFromResourceAsString("/converter/withCombinedRestrictionsSchema.avsc");
    final String jsonWithCombinedRestrictionsSchema = getFileFromResourceAsString("/converter/withCombinedRestrictionsSchema.json");

    final Map<String, Object> jsonSchema = mapper.readValue(avroWithCombinedRestrictionsSchema, new TypeReference<>() {
    });
    final Avro2JsonConvert converter = new Avro2JsonConvert();
    final JsonNode actual = converter.convertoToAirbyteJson( Jsons.deserialize(avroWithCombinedRestrictionsSchema));
    final JsonNode expect = mapper.readTree(jsonWithCombinedRestrictionsSchema);
    assertEquals(expect, actual);
  }


  @Test
  public void testConverterAvroWithArrayAndNestedRecordSchema() throws Exception {

    final String avroWithArrayAndNestedRecordSchema = getFileFromResourceAsString("/converter/withArrayAndNestedRecordSchema.avsc");
    final String jsonWithArrayAndNestedRecordSchema = getFileFromResourceAsString("/converter/withArrayAndNestedRecordSchema.json");

    final Map<String, Object> jsonSchema = mapper.readValue(avroWithArrayAndNestedRecordSchema, new TypeReference<>() {
    });
    final Avro2JsonConvert converter = new Avro2JsonConvert();
    final JsonNode actual = converter.convertoToAirbyteJson( Jsons.deserialize(avroWithArrayAndNestedRecordSchema));
    final JsonNode expect = mapper.readTree(jsonWithArrayAndNestedRecordSchema);
    assertEquals(expect, actual);
  }

  @Test
  public void testConverterAvroWithSchemaReference() throws Exception {

    final String avroWithSchemaReference = getFileFromResourceAsString("/converter/withSchemaReference.avsc");
    final String jsonWithSchemaReference = getFileFromResourceAsString("/converter/withSchemaReference.json");

    final Map<String, Object> jsonSchema = mapper.readValue(avroWithSchemaReference, new TypeReference<>() {
    });
    final Avro2JsonConvert converter = new Avro2JsonConvert();
    final JsonNode actual = converter.convertoToAirbyteJson( Jsons.deserialize(avroWithSchemaReference));
    final JsonNode expect = mapper.readTree(jsonWithSchemaReference);
    final String a = actual.toPrettyString();
    System.out.println(a);
    assertEquals(expect, actual);
  }


  @Test
  public void testConvertoToAirbyteJson() throws Exception {
    final String avroSimpleSchema = getFileFromResourceAsString("/converter/simpleSchema.avsc");
    final String jsonSimpleSchema = getFileFromResourceAsString("/converter/simpleSchema.json");
    final Avro2JsonConvert converter = new Avro2JsonConvert();
    final JsonNode actual = converter.convertoToAirbyteJson( Jsons.deserialize(avroSimpleSchema));
    final JsonNode expect = mapper.readTree(jsonSimpleSchema);
    assertEquals(expect, actual);
  }


  private String getFileFromResourceAsString(final String fileName) throws IOException {

    // The class loader that loaded the class
    final InputStream inputStream = getClass().getResourceAsStream(fileName);
    return IOUtils.toString(inputStream, Charset.defaultCharset());

  }




}
