package io.github.jsonflat;

import io.github.jsonflat.schema.JsonSchemaFactory;
import io.github.jsonflat.schema.Schema;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
/**
 * Center of Financial Technologies
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * @author Evgeniy Chukanov
 */

public class JsonSchemaTest {
  String jsonText = "{\n" +
    "  \"one\":1,\n" +
    "  \"two\":[\n" +
    "    {\"twenty\": 21},\n" +
    "    {\"twenty\": 22},\n" +
    "    {\"twenty\": 23}\n" +
    "  ],\n" +
    "  \"three\": \"3\"\n" +
    "}";

  @Test
  public void simpleTest() throws IOException {
    String schemaText = "{\n" +
      "  \"name\": \"test\",\n" +
      "  \"version\": \"1.0\",\n" +
      "  \"columns\": [\n" +
      "    {\"name\": \"one\"},\n" +
      "    {\n" +
      "      \"name\": \"two\",\n" +
      "      \"path\": \"two[*]\",\n" +
      "      \"columns\": [\n" +
      "        {\"name\": \"twenty\"}\n" +
      "      ]\n" +
      "    },\n" +
      "    {\"name\": \"three\"}\n" +
      "  ]\n" +
      "}\n";
    Schema schema = JsonSchemaFactory.builder().build()
      .generate(schemaText);
    Transformer transformer = new Transformer(schema);
    List<String> result = transformer.transform(jsonText);
    assertEquals(Arrays.asList(
      "{\"one\":1,\"two_twenty\":21,\"three\":\"3\"}",
      "{\"one\":1,\"two_twenty\":22,\"three\":\"3\"}",
      "{\"one\":1,\"two_twenty\":23,\"three\":\"3\"}"
    ), result);
  }

  @Test
  public void filterExistTest() throws IOException {
    List<String> jsonList = Arrays.asList(
      "{\"one1\":11,\"two\":[{\"twenty\":121},{\"twenty\":122}],\"three\":13}",
      "{\"one2\":21,\"two\":[{\"twenty\":221},{\"twenty\":222},{\"twenty3\":23}]}"
    );
    //Filter only "one1" json
    String schemaText = "{\n" +
      "  \"name\": \"test\",\n" +
      "  \"version\": \"1.0\",\n" +
      "  \"filter\": {\"class\":\"io.github.jsonflat.schema.filter.Exist\", \"path\":\"one1\"},\n"+
      "  \"columns\": [\n" +
      "    {\"name\": \"one1\"},\n" +
      "    {\n" +
      "      \"name\": \"two\",\n" +
      "      \"path\": \"two[*]\",\n" +
      "      \"columns\": [\n" +
      "        {\"name\": \"twenty\"}\n" +
      "      ]\n" +
      "    },\n" +
      "    {\"name\": \"three\"}\n" +
      "  ]\n" +
      "}\n";
    Schema schema = JsonSchemaFactory.builder().build()
      .generate(schemaText);
    Transformer transformer = new Transformer(schema);
    List<String> result = new ArrayList<>();
    for (String line: jsonList){
      result.addAll(transformer.transform(line));
    }
    assertEquals(Arrays.asList(
      "{\"one1\":11,\"two_twenty\":121,\"three\":13}",
      "{\"one1\":11,\"two_twenty\":122,\"three\":13}"
    ), result);
  }

  @Test
  public void filterNotExistTest() throws IOException {
    List<String> jsonList = Arrays.asList(
      "{\"one1\":11,\"two\":[{\"twenty\":121},{\"twenty\":122}],\"three\":13}",
      "{\"one2\":21,\"two\":[{\"twenty\":221},{\"twenty\":222},{\"twenty3\":23}]}"
    );
    //Filter only "one1" json
    String schemaText = "{\n" +
      "  \"name\": \"test\",\n" +
      "  \"version\": \"1.0\",\n" +
      "  \"filter\": {\"class\":\"io.github.jsonflat.schema.filter.NotExist\", \"path\":\"one1\"},\n"+
      "  \"columns\": [\n" +
      "    {\"name\": \"one2\"},\n" +
      "    {\n" +
      "      \"name\": \"two\",\n" +
      "      \"path\": \"two[*]\",\n" +
      "      \"columns\": [\n" +
      "        {\"name\": \"twenty\"}\n" +
      "      ]\n" +
      "    },\n" +
      "    {\"name\": \"three\"}\n" +
      "  ]\n" +
      "}\n";
    Schema schema = JsonSchemaFactory.builder().build()
      .generate(schemaText);
    Transformer transformer = new Transformer(schema);
    List<String> result = new ArrayList<>();
    for (String line: jsonList){
      result.addAll(transformer.transform(line));
    }
    assertEquals(Arrays.asList(
      "{\"one2\":21,\"two_twenty\":221}",
      "{\"one2\":21,\"two_twenty\":222}"
    ), result);
  }


  @Test
  public void testConvertString() throws IOException {
    String schemaText = "{\n" +
      "  \"name\": \"test\",\n" +
      "  \"version\": \"1.0\",\n" +
      "  \"columns\": [\n" +
      "    {\"name\": \"one\", \"converter\": {\"class\":\"io.github.jsonflat.schema.converter.ToString\"}},\n" +
      "    {\n" +
      "      \"name\": \"two\",\n" +
      "      \"path\": \"two[*]\",\n" +
      "      \"columns\": [\n" +
      "        {\"name\": \"twenty\"}\n" +
      "      ]\n" +
      "    },\n" +
      "    {\"name\": \"three\"}\n" +
      "  ]\n" +
      "}\n";
    Schema schema = JsonSchemaFactory.builder().build()
      .generate(schemaText);
    Transformer transformer = new Transformer(schema);
    List<String> result = transformer.transform(jsonText);
    assertEquals(Arrays.asList(
      "{\"one\":\"1\",\"two_twenty\":21,\"three\":\"3\"}",
      "{\"one\":\"1\",\"two_twenty\":22,\"three\":\"3\"}",
      "{\"one\":\"1\",\"two_twenty\":23,\"three\":\"3\"}"
    ), result);
  }


  @Test
  public void testNogroup() throws IOException {
    String jsonText = "{\n" +
      "  \"one\":1,\n" +
      "  \"two\":[\n" +
      "    {\"twenty\": 406015200000,\"twenty1\": [{\"twenty11\": 2011},{\"twenty11\": 2012}]},\n" +
      "    {\"twenty\": 383810400000},\n" +
      "    {\"twenty\": 1286406000000}\n" +
      "  ],\n" +
      "  \"three\": 3\n" +
      "}";

    String schemaText = "{\n" +
      "  \"name\": \"test\",\n" +
      "  \"version\": \"1.0\",\n" +
      "  \"columns\": [\n" +
      "    {\"name\": \"one\",\"converter\":{\"class\":\"io.github.jsonflat.schema.converter.ToString\"}},\n" +
      "    {\n" +
      "      \"name\": \"two\", \"path\": \"two[*]\", \"columns\": [\n" +
      "        {\"name\": \"twenty\", \"converter\":{\"class\":\"io.github.jsonflat.schema.converter.ToDatetime\"}},\n" +
      "        {\"name\": \"twenty1\", \"path\": \"twenty1[*]\", \"columns\": [\n" +
      "          {\"name\": \"twenty11\", \"converter\":{\"class\":\"io.github.jsonflat.schema.converter.ToString\"}}\n" +
      "        ]}\n" +
      "      ]\n" +
      "    },\n" +
      "    {\"name\": \"three\",\"converter\":{\"class\":\"io.github.jsonflat.schema.converter.ToLong\"}}\n" +
      "  ]\n" +
      "}\n";
    Schema schema = JsonSchemaFactory.builder().build()
      .generate(schemaText);
    Transformer transformer = new Transformer(schema);
    List<String> result = transformer.transform(jsonText);
    assertEquals(Arrays.asList(
      "{\"one\":\"1\",\"two_twenty\":\"1982-11-13T13:00:00\",\"two_twenty1_twenty11\":\"2011\",\"three\":3}",
      "{\"one\":\"1\",\"two_twenty\":\"1982-11-13T13:00:00\",\"two_twenty1_twenty11\":\"2012\",\"three\":3}",
      "{\"one\":\"1\",\"two_twenty\":\"1982-03-01T13:00:00\",\"three\":3}",
      "{\"one\":\"1\",\"two_twenty\":\"2010-10-07T06:00:00\",\"three\":3}"
    ), result);
  }

  @Test
  public void testArrayGroup() throws IOException {
    String jsonText = "{\n" +
      "  \"one\":1,\n" +
      "  \"two\":[\n" +
      "    {\"twenty\": 406015200000,\"twenty1\": [{\"twenty11\": 2011},{\"twenty11\": 2012}]},\n" +
      "    {\"twenty\": 383810400000},\n" +
      "    {\"twenty\": 1286406000000,\"twenty1\": [{\"twenty11\": 3011},{\"twenty11\": 3012}]}\n" +
      "  ],\n" +
      "  \"three\": 3\n" +
      "}";

    String schemaText = "{\n" +
      "  \"name\": \"test\",\n" +
      "  \"version\": \"1.0\",\n" +
      "  \"columns\": [\n" +
      "    {\"name\": \"one\",\"converter\":{\"class\":\"io.github.jsonflat.schema.converter.ToString\"}},\n" +
      "    {\n" +
      "      \"name\": \"two\", \"path\": \"two[*]\", \"columns\": [\n" +
      "        {\"name\": \"twenty\", \"converter\":{\"class\":\"io.github.jsonflat.schema.converter.ToDatetime\"}},\n" +
      "        {\"name\": \"twenty1array\", \"path\": \"twenty1[*].twenty11\",\"converter\":{\"class\":\"io.github.jsonflat.schema.converter.ToString\"}, \"group\":\"ARRAY\"}\n" +
      "      ]\n" +
      "    },\n" +
      "    {\"name\": \"three\",\"converter\":{\"class\":\"io.github.jsonflat.schema.converter.ToLong\"}}\n" +
      "  ]\n" +
      "}\n";
    Schema schema = JsonSchemaFactory.builder().build()
      .generate(schemaText);
    Transformer transformer = new Transformer(schema);
    List<String> result = transformer.transform(jsonText);
    assertEquals(Arrays.asList(
      "{\"one\":\"1\",\"two_twenty\":\"1982-11-13T13:00:00\",\"two_twenty1array\":[\"2011\",\"2012\"],\"three\":3}",
      "{\"one\":\"1\",\"two_twenty\":\"1982-03-01T13:00:00\",\"three\":3}",
      "{\"one\":\"1\",\"two_twenty\":\"2010-10-07T06:00:00\",\"two_twenty1array\":[\"3011\",\"3012\"],\"three\":3}"
    ), result);
  }

  @Test
  public void testArrayGroupJson() throws IOException {
    String jsonText = "{\n" +
      "  \"one\":1,\n" +
      "  \"two\":[\n" +
      "    {\"twenty\": 406015200000,\"twenty1\": [{\"twenty11\": 2011},{\"twenty11\": 2012}]},\n" +
      "    {\"twenty\": 383810400000},\n" +
      "    {\"twenty\": 1286406000000,\"twenty1\": [{\"twenty11\": 3011},{\"twenty11\": 3012}]}\n" +
      "  ],\n" +
      "  \"three\": 3\n" +
      "}";

    String schemaText = "{\n" +
      "  \"name\": \"test\",\n" +
      "  \"version\": \"1.0\",\n" +
      "  \"columns\": [\n" +
      "    {\"name\": \"one\"},\n" +
      "    {\n" +
      "      \"name\": \"two\", \"path\": \"two[*]\", \"columns\": [\n" +
      "        {\"name\": \"twenty\"},\n" +
      "        {\"name\": \"twenty1array\", \"path\": \"twenty1[*]\", \"group\":\"ARRAY\"}\n" +
      "      ]\n" +
      "    },\n" +
      "    {\"name\": \"three\",\"converter\":{\"class\":\"io.github.jsonflat.schema.converter.ToLong\"}}\n" +
      "  ]\n" +
      "}\n";
    Schema schema = JsonSchemaFactory.builder().build()
      .generate(schemaText);
    Transformer transformer = new Transformer(schema);
    List<String> result = transformer.transform(jsonText);
    assertEquals(Arrays.asList(
      "{\"one\":1,\"two_twenty\":406015200000,\"two_twenty1array\":[{\"twenty11\":2011},{\"twenty11\":2012}],\"three\":3}",
      "{\"one\":1,\"two_twenty\":383810400000,\"three\":3}",
      "{\"one\":1,\"two_twenty\":1286406000000,\"two_twenty1array\":[{\"twenty11\":3011},{\"twenty11\":3012}],\"three\":3}"
    ), result);
  }

  @Test
  public void testColumnGroupJsonLeaf() throws IOException {
    String jsonText = "{\n" +
      "  \"one\":1,\n" +
      "  \"two\":[\n" +
      "    {\"twenty\": 406015200000,\"twenty1\": [{\"twenty11\": 2011},{\"twenty11\": 2012}]},\n" +
      "    {\"twenty\": 383810400000},\n" +
      "    {\"twenty\": 1286406000000,\"twenty1\": [{\"twenty11\": 3011},{\"twenty11\": 3012}]}\n" +
      "  ],\n" +
      "  \"three\": 3\n" +
      "}";

    String schemaText = "{\n" +
      "  \"name\": \"test\",\n" +
      "  \"version\": \"1.0\",\n" +
      "  \"columns\": [\n" +
      "    {\"name\": \"one\"},\n" +
      "    {\n" +
      "      \"name\": \"two\", \"path\": \"two[*]\", \"columns\": [\n" +
      "        {\"name\": \"twenty\"},\n" +
      "        {\"name\": \"twenty1array\", \"path\": \"twenty1[*]\", \"group\":\"COLUMNS\"}\n" +
      "      ]\n" +
      "    },\n" +
      "    {\"name\": \"three\",\"converter\":{\"class\":\"io.github.jsonflat.schema.converter.ToLong\"}}\n" +
      "  ]\n" +
      "}\n";
    Schema schema = JsonSchemaFactory.builder().build()
      .generate(schemaText);
    Transformer transformer = new Transformer(schema);
    List<String> result = transformer.transform(jsonText);
    assertEquals(Arrays.asList(
      "{\"one\":1,\"two_twenty\":406015200000,\"two_twenty1array_0\":{\"twenty11\":2011},\"two_twenty1array_1\":{\"twenty11\":2012},\"three\":3}",
      "{\"one\":1,\"two_twenty\":383810400000,\"three\":3}",
      "{\"one\":1,\"two_twenty\":1286406000000,\"two_twenty1array_0\":{\"twenty11\":3011},\"two_twenty1array_1\":{\"twenty11\":3012},\"three\":3}"
    ), result);

  }

  @Test
  public void testColumnGroupJsonMiddle() throws IOException {
    String jsonText = "{\n" +
      "  \"one\":1,\n" +
      "  \"two\":[\n" +
      "    {\"mini\": 406015200000,\"micro\": [{\"sub\": 2011},{\"sub\": 2012}]},\n" +
      "    {\"mini\": 383810400000},\n" +
      "    {\"mini\": 1286406000000,\"micro\": [{\"sub\": 3011},{\"sub\": 3012}]}\n" +
      "  ],\n" +
      "  \"three\": 3\n" +
      "}";

    String schemaText = "{\n" +
      "  \"name\": \"test\",\n" +
      "  \"version\": \"1.0\",\n" +
      "  \"columns\": [\n" +
      "    {\"name\": \"one\"},\n" +
      "    {\n" +
      "      \"name\": \"two\", \"path\": \"two[*]\", \"group\":\"COLUMNS\", \"columns\": [\n" +
      "        {\"name\": \"mini\"},\n" +
      "        {\"name\": \"micro\", \"path\": \"micro[*]\"}\n" +
      "      ]\n" +
      "    },\n" +
      "    {\"name\": \"three\",\"converter\":{\"class\":\"io.github.jsonflat.schema.converter.ToLong\"}}\n" +
      "  ]\n" +
      "}\n";
    Schema schema = JsonSchemaFactory.builder().build()
      .generate(schemaText);
    Transformer transformer = new Transformer(schema);
    List<String> result = transformer.transform(jsonText);
    assertEquals(Arrays.asList(
      "{\"one\":1,\"two_0_mini\":406015200000,\"two_0_micro\":{\"sub\":2011},\"two_1_mini\":383810400000,\"two_2_mini\":1286406000000,\"two_2_micro\":{\"sub\":3011},\"three\":3}",
      "{\"one\":1,\"two_0_mini\":406015200000,\"two_0_micro\":{\"sub\":2012},\"two_1_mini\":383810400000,\"two_2_mini\":1286406000000,\"two_2_micro\":{\"sub\":3011},\"three\":3}",
      "{\"one\":1,\"two_0_mini\":406015200000,\"two_0_micro\":{\"sub\":2011},\"two_1_mini\":383810400000,\"two_2_mini\":1286406000000,\"two_2_micro\":{\"sub\":3012},\"three\":3}",
      "{\"one\":1,\"two_0_mini\":406015200000,\"two_0_micro\":{\"sub\":2012},\"two_1_mini\":383810400000,\"two_2_mini\":1286406000000,\"two_2_micro\":{\"sub\":3012},\"three\":3}"
    ), result);
  }

  @Test
  public void testColumnGroupJsonMiddle2() throws IOException {
    String jsonText = "{\n" +
      "  \"one\":1,\n" +
      "  \"two\":[\n" +
      "    {\"num\": 406015200000,\"dig\":21},\n" +
      "    {\"num\": 383810400000},\n" +
      "    {\"num\": 1286406000000,\"dig\":22}\n" +
      "  ],\n" +
      "  \"three\": 3\n" +
      "}";

    String schemaText = "{\n" +
      "  \"name\": \"test\",\n" +
      "  \"version\": \"1.0\",\n" +
      "  \"columns\": [\n" +
      "    {\"name\": \"one\"},\n" +
      "    {\n" +
      "      \"name\": \"two\", \"path\": \"two[*]\", \"group\":\"COLUMNS\", \"columns\": [\n" +
      "        {\"name\": \"num\"},\n" +
      "        {\"name\": \"dig\"}\n" +
      "      ]\n" +
      "    },\n" +
      "    {\"name\": \"three\"}\n" +
      "  ]\n" +
      "}\n";
    Schema schema = JsonSchemaFactory.builder().build()
      .generate(schemaText);
    Transformer transformer = new Transformer(schema);
    List<String> result = transformer.transform(jsonText);
    assertEquals(Collections.singletonList(
      "{\"one\":1,\"two_0_num\":406015200000,\"two_0_dig\":21,\"two_1_num\":383810400000,\"two_2_num\":1286406000000,\"two_2_dig\":22,\"three\":3}"
    ), result);

  }

  @Test
  public void testColumnGroupAll() throws IOException {
    String jsonText = "{\n" +
      "  \"one\":1,\n" +
      "  \"two\":[\n" +
      "    {\"mini\": 406015200000,\"micro\": [{\"sub\": 2011},{\"sub\": 2012}]},\n" +
      "    {\"mini\": 383810400000},\n" +
      "    {\"mini\": 1286406000000,\"micro\": [{\"sub\": 3011},{\"sub\": 3012}]}\n" +
      "  ],\n" +
      "  \"three\": 3\n" +
      "}";

    String schemaText = "{\n" +
      "  \"name\": \"test\",\n" +
      "  \"version\": \"1.0\",\n" +
      "  \"columns\": [\n" +
      "    {\"name\": \"one\"},\n" +
      "    {\n" +
      "      \"name\": \"two\", \"path\": \"two[*]\", \"group\":\"COLUMNS\", \"columns\": [\n" +
      "        {\"name\": \"mini\"},\n" +
      "        {\"name\": \"micro\", \"group\":\"COLUMNS\", \"path\": \"micro[*].sub\"}\n" +
      "      ]\n" +
      "    },\n" +
      "    {\"name\": \"three\",\"converter\":{\"class\":\"io.github.jsonflat.schema.converter.ToLong\"}}\n" +
      "  ]\n" +
      "}\n";
    Schema schema = JsonSchemaFactory.builder().build()
      .generate(schemaText);
    Transformer transformer = new Transformer(schema);
    List<String> result = transformer.transform(jsonText);
    assertEquals(Collections.singletonList(
      "{\"one\":1,\"two_0_mini\":406015200000,\"two_0_micro_0\":2011,\"two_0_micro_1\":2012,\"two_1_mini\":383810400000,\"two_2_mini\":1286406000000,\"two_2_micro_0\":3011,\"two_2_micro_1\":3012,\"three\":3}"
    ), result);
  }

  @Test
  public void testConcatGroup() throws IOException {
    String jsonText = "{\n" +
      "  \"one\":1,\n" +
      "  \"two\":[\n" +
      "    {\"twenty\": 406015200000,\"twenty1\": [{\"twenty11\": 2011},{\"twenty11\": 2012}]},\n" +
      "    {\"twenty\": 383810400000},\n" +
      "    {\"twenty\": 1286406000000,\"twenty1\": [{\"twenty11\": 3011},{\"twenty11\": 3012}]}\n" +
      "  ],\n" +
      "  \"three\": 3\n" +
      "}";

    String schemaText = "{\n" +
      "  \"name\": \"test\",\n" +
      "  \"version\": \"1.0\",\n" +
      "  \"columns\": [\n" +
      "    {\"name\": \"one\",\"converter\":{\"class\":\"io.github.jsonflat.schema.converter.ToString\"}},\n" +
      "    {\n" +
      "      \"name\": \"two\", \"path\": \"two[*]\", \"columns\": [\n" +
      "        {\"name\": \"twenty\", \"converter\":{\"class\":\"io.github.jsonflat.schema.converter.ToDatetime\"}},\n" +
      "        {\"name\": \"twenty1array\", \"path\": \"twenty1[*].twenty11\",\"converter\":{\"class\":\"io.github.jsonflat.schema.converter.ToString\"}, \"group\":\"CONCAT\"}\n" +
      "      ]\n" +
      "    },\n" +
      "    {\"name\": \"three\",\"converter\":{\"class\":\"io.github.jsonflat.schema.converter.ToLong\"}}\n" +
      "  ]\n" +
      "}\n";
    Schema schema = JsonSchemaFactory.builder().build()
      .generate(schemaText);
    Transformer transformer = new Transformer(schema);
    List<String> result = transformer.transform(jsonText);
    assertEquals(Arrays.asList(
      "{\"one\":\"1\",\"two_twenty\":\"1982-11-13T13:00:00\",\"two_twenty1array\":\"2011,2012\",\"three\":3}",
      "{\"one\":\"1\",\"two_twenty\":\"1982-03-01T13:00:00\",\"three\":3}",
      "{\"one\":\"1\",\"two_twenty\":\"2010-10-07T06:00:00\",\"two_twenty1array\":\"3011,3012\",\"three\":3}"
    ), result);
  }
  @Test
  public void testFullanme() throws IOException {
    String jsonText = "{\n" +
      "  \"one\":1,\n" +
      "  \"two\":[\n" +
      "    {\"twenty\": 406015200000,\"twenty1\": [{\"twenty11\": 2011},{\"twenty11\": 2012}]},\n" +
      "    {\"twenty\": 383810400000},\n" +
      "    {\"twenty\": 1286406000000,\"twenty1\": [{\"twenty11\": 3011},{\"twenty11\": 3012}]}\n" +
      "  ],\n" +
      "  \"three\": 3\n" +
      "}";

    String schemaText = "{\n" +
      "  \"name\": \"test\",\n" +
      "  \"version\": \"1.0\",\n" +
      "  \"columns\": [\n" +
      "    {\"name\": \"one\",\"converter\":{\"class\":\"io.github.jsonflat.schema.converter.ToString\"}},\n" +
      "    {\n" +
      "      \"name\": \"two\", \"path\": \"two[*]\", \"columns\": [\n" +
      "        {\"name\": \"twenty\", \"converter\":{\"class\":\"io.github.jsonflat.schema.converter.ToDatetime\"}},\n" +
      "        {\"name\": \"twenty1array\", \"path\": \"twenty1[*].twenty11\",\"fullname\":\"true\"}\n" +
      "      ]\n" +
      "    },\n" +
      "    {\"name\": \"three\",\"converter\":{\"class\":\"io.github.jsonflat.schema.converter.ToLong\"}}\n" +
      "  ]\n" +
      "}\n";
    Schema schema = JsonSchemaFactory.builder().build()
      .generate(schemaText);
    Transformer transformer = new Transformer(schema);
    List<String> result = transformer.transform(jsonText);
    assertEquals(Arrays.asList(
      "{\"one\":\"1\",\"two_twenty\":\"1982-11-13T13:00:00\",\"twenty1array\":2011,\"three\":3}",
      "{\"one\":\"1\",\"two_twenty\":\"1982-11-13T13:00:00\",\"twenty1array\":2012,\"three\":3}",
      "{\"one\":\"1\",\"two_twenty\":\"1982-03-01T13:00:00\",\"three\":3}",
      "{\"one\":\"1\",\"two_twenty\":\"2010-10-07T06:00:00\",\"twenty1array\":3011,\"three\":3}",
      "{\"one\":\"1\",\"two_twenty\":\"2010-10-07T06:00:00\",\"twenty1array\":3012,\"three\":3}"
    ), result);
  }

  @Test
  public void testToDateCustom() throws IOException {
    String jsonText = "{\n" +
      "  \"one\":1,\n" +
      "  \"two\":[\n" +
      "    {\"twenty\": 406015200000,\"twenty1\": [{\"twenty11\": 2011},{\"twenty11\": 2012}]},\n" +
      "    {\"twenty\": 383810400000},\n" +
      "    {\"twenty\": 1286406000000,\"twenty1\": [{\"twenty11\": 3011},{\"twenty11\": 3012}]}\n" +
      "  ],\n" +
      "  \"three\": 3\n" +
      "}";

    String schemaText = "{\n" +
      "  \"name\": \"test\",\n" +
      "  \"version\": \"1.0\",\n" +
      "  \"columns\": [\n" +
      "    {\"name\": \"one\",\"converter\":{\"class\":\"io.github.jsonflat.schema.converter.ToString\"}},\n" +
      "    {\n" +
      "      \"name\": \"two\", \"path\": \"two[*]\", \"columns\": [\n" +
      "        {\"name\": \"twenty\", \"converter\":{\"class\":\"io.github.jsonflat.schema.converter.ToDatetime\",\"pattern\":\"yyyy-MM-dd\"}},\n" +
      "        {\"name\": \"twenty1array\", \"path\": \"twenty1[*].twenty11\",\"fullname\":\"true\"}\n" +
      "      ]\n" +
      "    },\n" +
      "    {\"name\": \"three\",\"converter\":{\"class\":\"io.github.jsonflat.schema.converter.ToLong\"}}\n" +
      "  ]\n" +
      "}\n";
    Schema schema = JsonSchemaFactory.builder().build()
      .generate(schemaText);
    Transformer transformer = new Transformer(schema);
    List<String> result = transformer.transform(jsonText);
    assertEquals(Arrays.asList(
      "{\"one\":\"1\",\"two_twenty\":\"1982-11-13\",\"twenty1array\":2011,\"three\":3}",
      "{\"one\":\"1\",\"two_twenty\":\"1982-11-13\",\"twenty1array\":2012,\"three\":3}",
      "{\"one\":\"1\",\"two_twenty\":\"1982-03-01\",\"three\":3}",
      "{\"one\":\"1\",\"two_twenty\":\"2010-10-07\",\"twenty1array\":3011,\"three\":3}",
      "{\"one\":\"1\",\"two_twenty\":\"2010-10-07\",\"twenty1array\":3012,\"three\":3}"
    ), result);
  }

  @Test
  public void testToDateBad() throws IOException {
    String jsonText = "{\n" +
      "  \"one\":1,\n" +
      "  \"two\":[\n" +
      "    {\"twenty\": \"a406015200000\",\"twenty1\": [{\"twenty11\": 2011},{\"twenty11\": 2012}]},\n" +
      "    {\"twenty\": \"b383810400000\"},\n" +
      "    {\"twenty\": \"c1286406000000\",\"twenty1\": [{\"twenty11\": 3011},{\"twenty11\": 3012}]}\n" +
      "  ],\n" +
      "  \"three\": 3\n" +
      "}";

    String schemaText = "{\n" +
      "  \"name\": \"test\",\n" +
      "  \"version\": \"1.0\",\n" +
      "  \"columns\": [\n" +
      "    {\"name\": \"one\",\"converter\":{\"class\":\"io.github.jsonflat.schema.converter.ToString\"}},\n" +
      "    {\n" +
      "      \"name\": \"two\", \"path\": \"two[*]\", \"columns\": [\n" +
      "        {\"name\": \"twenty\", \"converter\":{\"class\":\"io.github.jsonflat.schema.converter.ToDatetime\",\"pattern\":\"yyyy-MM-dd\"}},\n" +
      "        {\"name\": \"twenty1array\", \"path\": \"twenty1[*].twenty11\",\"fullname\":\"true\"}\n" +
      "      ]\n" +
      "    },\n" +
      "    {\"name\": \"three\",\"converter\":{\"class\":\"io.github.jsonflat.schema.converter.ToLong\"}}\n" +
      "  ]\n" +
      "}\n";
    Schema schema = JsonSchemaFactory.builder().build()
      .generate(schemaText);
    Transformer transformer = new Transformer(schema);
    List<String> result = transformer.transform(jsonText);
    assertEquals(Arrays.asList(
      "{\"one\":\"1\",\"two_twenty\":\"a406015200000\",\"twenty1array\":2011,\"three\":3}",
      "{\"one\":\"1\",\"two_twenty\":\"a406015200000\",\"twenty1array\":2012,\"three\":3}",
      "{\"one\":\"1\",\"two_twenty\":\"b383810400000\",\"three\":3}",
      "{\"one\":\"1\",\"two_twenty\":\"c1286406000000\",\"twenty1array\":3011,\"three\":3}",
      "{\"one\":\"1\",\"two_twenty\":\"c1286406000000\",\"twenty1array\":3012,\"three\":3}"
    ), result);
  }

  @Test
  public void testRequiredRow() throws IOException {
    String jsonText = "{\n" +
      "  \"one\":1,\n" +
      "  \"two\":[\n" +
      "    {\"twenty\": 406015200000,\"twenty1\": [{\"twenty11\": 2011},{\"twenty11\": 2012}]},\n" +
      "    {\"twenty\": 383810400000},\n" +
      "    {\"twenty\": 1286406000000,\"twenty1\": [{\"twenty11\": 2013}]}\n" +
      "  ],\n" +
      "  \"three\": 3\n" +
      "}";

    String schemaText = "{\n" +
      "  \"name\": \"test\",\n" +
      "  \"version\": \"1.0\",\n" +
      "  \"columns\": [\n" +
      "    {\"name\": \"one\"},\n" +
      "    {\n" +
      "      \"name\": \"two\", \"path\": \"two[*]\", \"columns\": [\n" +
      "        {\"name\": \"twenty\"},\n" +
      "        {\"name\": \"twenty1\", \"path\": \"twenty1[*]\", \"columns\": [\n" +
      "          {\"name\": \"twenty11\",\"skipRowIfEmpty\":true}\n" +
      "        ]}\n" +
      "      ]\n" +
      "    },\n" +
      "    {\"name\": \"three\",\"converter\":{\"class\":\"io.github.jsonflat.schema.converter.ToLong\"}}\n" +
      "  ]\n" +
      "}\n";
    Schema schema = JsonSchemaFactory.builder().build()
      .generate(schemaText);
    Transformer transformer = new Transformer(schema);
    List<String> result = transformer.transform(jsonText);
    assertEquals(Arrays.asList(
      "{\"one\":1,\"two_twenty\":406015200000,\"two_twenty1_twenty11\":2011,\"three\":3}",
      "{\"one\":1,\"two_twenty\":406015200000,\"two_twenty1_twenty11\":2012,\"three\":3}",
      "{\"one\":1,\"two_twenty\":383810400000,\"three\":3}",
      "{\"one\":1,\"two_twenty\":1286406000000,\"two_twenty1_twenty11\":2013,\"three\":3}"
    ), result);
  }


  @Test
  public void testSkipJson() throws IOException {
    List<String> jsonTextList = Arrays.asList(
      "{\n" +
      "  \"one\":11,\n" +
      "  \"two\":[\n" +
      "    {\"twenty\": 121},\n" +
      "    {\"twenty\": 122},\n" +
      "    {\"twenty\": 123}\n" +
      "  ],\n" +
      "  \"three\": 13\n" +
      "}",
      "{\n" +
        "  \"two\":[\n" +
        "    {\"twenty\": 221},\n" +
        "    {\"twenty\": 222},\n" +
        "    {\"twenty\": 223}\n" +
        "  ],\n" +
        "  \"three\": 23\n" +
        "}",
      "{\n" +
        "  \"one\":31,\n" +
        "  \"two\":[\n" +
        "    {\"twenty\": 321},\n" +
        "    {\"twenty\": 322},\n" +
        "    {\"twenty\": 323}\n" +
        "  ]\n" +
        "}");

    String schemaText = "{\n" +
      "  \"name\": \"test\",\n" +
      "  \"version\": \"1.0\",\n" +
      "  \"columns\": [\n" +
      "    {\"name\": \"one\",\"skipJsonIfEmpty\": \"true\"},\n" +
      "    {\n" +
      "      \"name\": \"two\",\n" +
      "      \"path\": \"two[*]\",\n" +
      "      \"columns\": [\n" +
      "        {\"name\": \"twenty\"}\n" +
      "      ]\n" +
      "    },\n" +
      "    {\"name\": \"three\",\"skipJsonIfEmpty\": \"true\"}\n" +
      "  ]\n" +
      "}\n";
    Schema schema = JsonSchemaFactory.builder().build()
      .generate(schemaText);
    Transformer transformer = new Transformer(schema);
    List<String> result = new ArrayList<>();
    jsonTextList.forEach(
      json -> {
        try {
          result.addAll(transformer.transform(json));
        } catch (IOException e) {
          //
        }
      }
    );
    assertEquals(Arrays.asList(
      "{\"one\":11,\"two_twenty\":121,\"three\":13}",
        "{\"one\":11,\"two_twenty\":122,\"three\":13}",
        "{\"one\":11,\"two_twenty\":123,\"three\":13}"
    ), result);

  }

}
