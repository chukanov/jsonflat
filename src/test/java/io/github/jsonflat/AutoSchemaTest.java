package io.github.jsonflat;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.jsonflat.schema.AutoSchemaFactory;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;
import io.github.jsonflat.schema.Schema;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static io.github.jsonflat.Transformer.MAPPER;

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

public class AutoSchemaTest {
    String jsonText = "{\n" +
            "  \"one\":1,\n" +
            "  \"two\":[\n" +
            "    {\"twenty1\": 21},\n" +
            "    {\"twenty2\": 22},\n" +
            "    {\"twenty3\": 23}\n" +
            "  ],\n" +
            "  \"three\": 3\n" +
            "}";
    @Test
    public void testFilterOneTwoWild() throws IOException {
        Schema schema = AutoSchemaFactory.builder()
                .columnStringFilters(Arrays.asList("one", "two*"))
                .build()
                .generate(jsonText);
        Transformer transformer = new Transformer(schema);
        List<String> result = transformer.transform(jsonText);
        assertEquals(Arrays.asList(
          "{\"one\":1,\"two_twenty1\":21}",
          "{\"one\":1,\"two_twenty2\":22}",
          "{\"one\":1,\"two_twenty3\":23}"
        ), result);
    }

    @Test
    public void testFilterOneTwo() throws IOException {
        Schema schema = AutoSchemaFactory.builder()
          .columnStringFilters(Arrays.asList("one", "two"))
          .build()
          .generate(jsonText);
        Transformer transformer = new Transformer(schema);
        List<String> result = transformer.transform(jsonText);
        assertEquals(Collections.singletonList(
          "{\"one\":1,\"two\":[{\"twenty1\":21},{\"twenty2\":22},{\"twenty3\":23}]}"
        ), result);
    }

    @Test
    public void testFilterOneTwoArray() throws IOException {
        Schema schema = AutoSchemaFactory.builder()
          .columnArrayFilters(new String[]{"one", "two"})
          .build()
          .generate(jsonText);
        Transformer transformer = new Transformer(schema);
        List<String> result = transformer.transform(jsonText);
        assertEquals(Arrays.asList(
          "{\"one\":1,\"two_twenty1\":21}",
          "{\"one\":1,\"two_twenty2\":22}",
          "{\"one\":1,\"two_twenty3\":23}"
        ), result);
    }

    @Test
    public void testFilterTwo() throws IOException {
        Schema schema = AutoSchemaFactory.builder()
                .columnStringFilters(Collections.singletonList("two*"))
                .build()
                .generate(jsonText);
        Transformer transformer = new Transformer(schema);
        List<String> result = transformer.transform(jsonText);
        assertEquals(Arrays.asList(
          "{\"two_twenty1\":21}",
          "{\"two_twenty2\":22}",
          "{\"two_twenty3\":23}"
        ), result);
    }

    @Test
    public void testFilterTwoTwenty2() throws IOException {
        Schema schema = AutoSchemaFactory.builder()
                .columnStringFilters(Collections.singletonList("two_twenty2"))
                .build()
                .generate(jsonText);
        Transformer transformer = new Transformer(schema);
        List<String> result = transformer.transform(jsonText);
        assertEquals(Collections.singletonList("{\"two_twenty2\":22}"), result);
    }

    @Test
    public void testFilterOneTwoTwenty2Array() throws IOException {
        Schema schema = AutoSchemaFactory.builder()
          .columnArrayFilters(new Object[]{"three", new String[]{"two", "twenty2"}})
          .build()
          .generate(jsonText);
        Transformer transformer = new Transformer(schema);
        List<String> result = transformer.transform(jsonText);
        assertEquals(
          Collections.singletonList("{\"two_twenty2\":22,\"three\":3}"),
          result
        );
    }

    @Test
    public void testFilterOneTwoTwenty2() throws IOException {
        Schema schema = AutoSchemaFactory.builder()
                .columnStringFilters(Arrays.asList("three","two_twenty2"))
                .build()
                .generate(jsonText);
        Transformer transformer = new Transformer(schema);
        List<String> result = transformer.transform(jsonText);
        assertEquals(
          Collections.singletonList("{\"two_twenty2\":22,\"three\":3}"),
          result
        );
    }

    @Test
    public void testFilterOne() throws IOException {
        Schema schema = AutoSchemaFactory.builder()
                .columnStringFilters(Collections.singletonList("one"))
                .build()
                .generate(jsonText);
        Transformer transformer = new Transformer(schema);
        List<String> result = transformer.transform(jsonText);
        assertEquals(Collections.singletonList("{\"one\":1}"), result);
    }

    @Test
    public void testFilterFour() throws IOException {
        Schema schema = AutoSchemaFactory.builder()
                .columnStringFilters(Collections.singletonList("four"))
                .build()
                .generate(jsonText);
        Transformer transformer = new Transformer(schema);
        List<String> result = transformer.transform(jsonText);
        assertEquals(Collections.emptyList(), result);
    }

    @Test
    public void testNoFilter() throws IOException {
        String jsonText = "{\n" +
          "  \"one\":1,\n" +
          "  \"two\":[\n" +
          "    {\"twenty\": 21},\n" +
          "    {\"twenty\": 22},\n" +
          "    {\"twenty\": 23}\n" +
          "  ],\n" +
          "  \"three\": 3\n" +
          "}";
        Schema schema = AutoSchemaFactory.builder()
                .build()
                .generate(jsonText);
        Transformer transformer = new Transformer(schema);
        List<String> result = transformer.transform(jsonText);
        assertEquals(Arrays.asList(
          "{\"one\":1,\"two_twenty\":21,\"three\":3}",
          "{\"one\":1,\"two_twenty\":22,\"three\":3}",
          "{\"one\":1,\"two_twenty\":23,\"three\":3}"
        ), result);
    }

    @Test
    public void testNoFilter2() throws IOException {
        val test = "{}";
        Schema schema = AutoSchemaFactory.builder()
                .build()
                .generate(test);
        Transformer transformer = new Transformer(schema);
        List<String> result = transformer.transform(test);
        assertEquals(Collections.emptyList(), result);
    }

    @Test
    public void testNoFilterNull() throws IOException {
        String test = null;
        Schema schema = AutoSchemaFactory.builder()
                .build()
                .generate(test);
        Transformer transformer = new Transformer(schema);
        List<String> result = transformer.transform(test);
        assertEquals(Collections.emptyList(), result);
    }

    @Test(expected = com.fasterxml.jackson.core.JsonParseException.class)
    public void testBadJson() throws IOException {
        String test = "hello world!";
        Schema schema = AutoSchemaFactory.builder()
                .build()
                .generate(test);
        Transformer transformer = new Transformer(schema);
        transformer.transform(test);
    }

    @Test
    public void testNested() throws IOException {
        val js ="{\n" +
          "  \"foo1\": {\n" +
          "    \"v\": \"11\",\n" +
          "    \"e\": [\n" +
          "      {\n" +
          "        \"foo11\": {\n" +
          "          \"v\": \"111\",\n" +
          "          \"e\": [\"xxx1\",\"yyy1\"]\n" +
          "        },\n" +
          "        \"foo12\": {\n" +
          "          \"v\": \"112\",\n" +
          "          \"e\": [\"xxx2\",\"yyy2\"]\n" +
          "        },\n" +
          "        \"foo13\": 113\n" +
          "      }, {\n" +
          "        \"foo13\": 123\n" +
          "      }\n" +
          "    ]\n" +
          "  },\n" +
          "  \"foo2\": 12,\n" +
          "  \"foo3\": 13\n" +
          "}";
        Schema schema = AutoSchemaFactory.builder()
                .primitiveArraysGroup(Schema.GroupPolicy.NO_GROUP)
                .build()
                .generate(js);
        Transformer transformer = new Transformer(schema);
        List<String> result = transformer.transform(js);
        Assert.assertEquals(Arrays.asList(
          "{\"foo1_v\":\"11\",\"foo1_e_foo11_v\":\"111\",\"foo1_e_foo11_e\":\"xxx1\",\"foo1_e_foo12_v\":\"112\",\"foo1_e_foo12_e\":\"xxx2\",\"foo1_e_foo13\":113,\"foo2\":12,\"foo3\":13}",
          "{\"foo1_v\":\"11\",\"foo1_e_foo11_v\":\"111\",\"foo1_e_foo11_e\":\"yyy1\",\"foo1_e_foo12_v\":\"112\",\"foo1_e_foo12_e\":\"xxx2\",\"foo1_e_foo13\":113,\"foo2\":12,\"foo3\":13}",
          "{\"foo1_v\":\"11\",\"foo1_e_foo11_v\":\"111\",\"foo1_e_foo11_e\":\"xxx1\",\"foo1_e_foo12_v\":\"112\",\"foo1_e_foo12_e\":\"yyy2\",\"foo1_e_foo13\":113,\"foo2\":12,\"foo3\":13}",
          "{\"foo1_v\":\"11\",\"foo1_e_foo11_v\":\"111\",\"foo1_e_foo11_e\":\"yyy1\",\"foo1_e_foo12_v\":\"112\",\"foo1_e_foo12_e\":\"yyy2\",\"foo1_e_foo13\":113,\"foo2\":12,\"foo3\":13}",
          "{\"foo1_v\":\"11\",\"foo1_e_foo13\":123,\"foo2\":12,\"foo3\":13}"),
          result);
    }

    @Test
    public void testNoArrays() throws IOException {
        val js ="{\n" +
                "  \"one\": {\"sub1\": 1},\n" +
                "  \"two\": 2\n" +
                "}";
        Schema schema = AutoSchemaFactory.builder()
                .build()
                .generate(js);
        Transformer transformer = new Transformer(schema);
        List<String> result = transformer.transform(js);
        Assert.assertEquals(Collections.singletonList("{\"one_sub1\":1,\"two\":2}"), result);
    }

    @Test
    public void testSimpleArraysAsArray() throws IOException {
        val js ="{\n" +
          "  \"one\": {\"sub1\": [1,2,3]},\n" +
          "  \"two\": 2\n" +
          "}";
        Schema schema = AutoSchemaFactory.builder()
          .primitiveArraysGroup(Schema.GroupPolicy.ARRAY)
          .build()
          .generate(js);
        Transformer transformer = new Transformer(schema);
        List<String> result = transformer.transform(js);
        Assert.assertEquals(
          Collections.singletonList("{\"one_sub1\":[1,2,3],\"two\":2}"),
          result
        );
    }

    @Test
    public void testSimpleArraysAsColumns() throws IOException {
        val js ="{\n" +
          "  \"one\": {\"sub1\": [1,2,3]},\n" +
          "  \"two\": 2\n" +
          "}";
        Schema schema = AutoSchemaFactory.builder()
          .primitiveArraysGroup(Schema.GroupPolicy.COLUMNS)
          .build()
          .generate(js);
        Transformer transformer = new Transformer(schema);
        List<String> result = transformer.transform(js);
        Assert.assertEquals(
          Collections.singletonList("{\"one_sub1_0\":1,\"one_sub1_1\":2,\"one_sub1_2\":3,\"two\":2}"),
          result
        );
    }

    @Test
    public void testComplexArraysNoGroup() throws IOException {
        val js ="{\n" +
          "  \"one\": {\"sub1\": [1,true,{\"foo\":1}, {\"bar\":[3,4]}]},\n" +
          "  \"two\": 2\n" +
          "}";
        val node = MAPPER.readValue(js, JsonNode.class);
        Schema schema = AutoSchemaFactory.builder()
          .primitiveArraysGroup(Schema.GroupPolicy.NO_GROUP)
          .build()
          .generate(node);
        Transformer transformer = new Transformer(schema);
        val result = transformer.transform(js);
        assertEquals(
          Arrays.asList(
            "{\"one_sub1\":1,\"two\":2}",
            "{\"one_sub1\":true,\"two\":2}",
            "{\"one_sub1_foo\":1,\"two\":2}",
            "{\"one_sub1_bar\":3,\"two\":2}",
            "{\"one_sub1_bar\":4,\"two\":2}"
          )
          ,result);
    }

    @Test
    public void testComplexArraysArrayGroup() throws IOException {
        val js ="{\n" +
          "  \"one\": {\"sub1\": [1,true,{\"foo\":1}, {\"bar\":[3,4]}]},\n" +
          "  \"two\": 2\n" +
          "}";
        val node = MAPPER.readValue(js, JsonNode.class);
        Schema schema = AutoSchemaFactory.builder()
          .primitiveArraysGroup(Schema.GroupPolicy.ARRAY)
          .build()
          .generate(node);
        Transformer transformer = new Transformer(schema);
        val result = transformer.transform(js);
        assertEquals(
          Arrays.asList(
            "{\"one_sub1\":1,\"two\":2}",
            "{\"one_sub1\":true,\"two\":2}",
            "{\"one_sub1_foo\":1,\"two\":2}",
            "{\"one_sub1_bar\":[3,4],\"two\":2}"
          )
          ,result);
    }

    @Test
    public void testColumnGroupJsonMiddle() throws IOException {
        String jsonText = "{\n" +
          "  \"one\":1,\n" +
          "  \"two\":[\n" +
          "    {\"num\": 406015200000,\"dig\":21},\n" +
          "    {\"num\": 383810400000},\n" +
          "    {\"num\": 1286406000000,\"dig\":22}\n" +
          "  ],\n" +
          "  \"three\": 3\n" +
          "}";

        Schema schema = AutoSchemaFactory.builder()
          .complexArraysGroup(Schema.GroupPolicy.COLUMNS)
          .build()
          .generate(jsonText);
        Transformer transformer = new Transformer(schema);
        List<String> result = transformer.transform(jsonText);
        assertEquals(
          Collections.singletonList("{\"one\":1,\"two_0_num\":406015200000,\"two_0_dig\":21,\"two_1_num\":383810400000,\"two_2_num\":1286406000000,\"two_2_dig\":22,\"three\":3}"),
          result
        );
    }


    @Test
    public void testComplexArraysConcatGroup() throws IOException {
        val js ="{\n" +
          "  \"one\": {\"sub1\": [1,true,{\"foo\":1}, {\"bar\":[3,4]}]},\n" +
          "  \"two\": 2\n" +
          "}";
        val node = MAPPER.readValue(js, JsonNode.class);
        Schema schema = AutoSchemaFactory.builder()
          .primitiveArraysGroup(Schema.GroupPolicy.CONCAT)
          .build()
          .generate(node);
        Transformer transformer = new Transformer(schema);
        val result = transformer.transform(js);
        assertEquals(
          Arrays.asList(
            "{\"one_sub1\":1,\"two\":2}",
            "{\"one_sub1\":true,\"two\":2}",
            "{\"one_sub1_foo\":1,\"two\":2}",
            "{\"one_sub1_bar\":\"3,4\",\"two\":2}"
          )
          ,result);
    }

    @Test
    public void testComplexArrayOfArrays() throws IOException {
        val js ="{\n" +
          "  \"one\": {\"sub\": [1,true,{\"foo\":1}, [5,[51,52],6,[{\"a\":1},{\"b\":2}]],{\"bar\":[3,4]},\"xxx\"]},\n" +
          "  \"two\": 2\n" +
          "}";
        val node = MAPPER.readValue(js, JsonNode.class);
        Schema schema = AutoSchemaFactory.builder()
          .primitiveArraysGroup(Schema.GroupPolicy.NO_GROUP)
          .complexArraysGroup(Schema.GroupPolicy.NO_GROUP)
          .build()
          .generate(node);
        Transformer transformer = new Transformer(schema);
        val result = transformer.transform(js);
        //result.forEach(System.out::println);
    }

    @Test
    public void testStructArrayOfArraysMiddleGroupLeafNoGroup() throws IOException {
        val js ="{\n" +
          "  \"one\": {\"sub\": [1, 2, {\"foo\":1},{\"bar\":[3,4]},\"xxx\"]},\n" +
          "  \"two\": 2\n" +
          "}";
        val node = MAPPER.readValue(js, JsonNode.class);
        Schema schema = AutoSchemaFactory.builder()
          .primitiveArraysGroup(Schema.GroupPolicy.NO_GROUP)
          .complexArraysGroup(Schema.GroupPolicy.COLUMNS)
          .build()
          .generate(node);
        Transformer transformer = new Transformer(schema);
        val result = transformer.transform(js);
        Assert.assertEquals(
          Arrays.asList(
            "{\"one_sub_0_0\":1,\"one_sub_1_0\":2,\"one_sub_2_foo\":1,\"one_sub_3_bar\":3,\"one_sub_4_0\":\"xxx\",\"two\":2}",
            "{\"one_sub_0_0\":1,\"one_sub_1_0\":2,\"one_sub_2_foo\":1,\"one_sub_3_bar\":4,\"one_sub_4_0\":\"xxx\",\"two\":2}"
          ),
          result
        );
    }

    @Test
    public void testStructArrayOfArraysMiddleGroupLeafColumns() throws IOException {
        val js ="{\n" +
          "  \"one\": {\"sub\": [1, 2, {\"foo\":1},{\"bar\":[3,4]},\"xxx\"]},\n" +
          "  \"two\": 2\n" +
          "}";
        val node = MAPPER.readValue(js, JsonNode.class);
        Schema schema = AutoSchemaFactory.builder()
          .primitiveArraysGroup(Schema.GroupPolicy.COLUMNS)
          .complexArraysGroup(Schema.GroupPolicy.COLUMNS)
          .build()
          .generate(node);
        Transformer transformer = new Transformer(schema);
        val result = transformer.transform(js);
        Assert.assertEquals(
          Collections.singletonList(
            "{\"one_sub_0_0\":1,\"one_sub_1_0\":2,\"one_sub_2_foo\":1,\"one_sub_3_bar_0\":3,\"one_sub_3_bar_1\":4,\"one_sub_4_0\":\"xxx\",\"two\":2}"
          ),
          result
        );
    }

    @Test
    public void testSimpleArraysConcat() throws IOException {
        val js ="{\n" +
          "  \"one\": {\"sub1\": [1,2,3,4,5,6,7]},\n" +
          "  \"two\": 2\n" +
          "}";
        Schema schema = AutoSchemaFactory.builder()
          .primitiveArraysGroup(Schema.GroupPolicy.CONCAT)
          .build()
          .generate(js);
        Transformer transformer = new Transformer(schema);
        List<String> result = transformer.transform(js);
        Assert.assertEquals(
          Collections.singletonList("{\"one_sub1\":\"1,2,3,4,5,6,7\",\"two\":2}"),
          result
        );
    }

    @Test
    public void testInputArray() throws IOException {
        val js ="[{\"one\": {\"sub1\": 1},\"two\": 2},{\"one\": {\"sub1\": 11}},{\"three\":3}]";
        Schema schema = AutoSchemaFactory.builder()
          .build()
          .generate(js);
        Transformer transformer = new Transformer(schema);
        List<String> result = transformer.transform(js);
        Assert.assertEquals(
          Arrays.asList(
            "{\"one_sub1\":1,\"two\":2}",
            "{\"one_sub1\":11}",
            "{\"three\":3}"
          ), result);
    }

    @Test
    public void testInputStrangeArray() throws IOException {
        val js ="[[11,12,13],[21,22],[[3],[4]]]";
        Schema schema = AutoSchemaFactory.builder()
          .build()
          .generate(js);
        Transformer transformer = new Transformer(schema);
        List<String> result = transformer.transform(js);
        //result.forEach(System.out::println);
    }

    public void testDota() throws IOException {
        String json = new String (Files.readAllBytes(Paths.get("src/test/resources/dota.json")));
        Schema schema = AutoSchemaFactory.builder()
          .columnStringFilters(Arrays.asList("players*"))
          .build()
          .generate(json);
        Transformer transformer = new Transformer(schema);
        List<String> result = transformer.transform(json);
        //result.forEach(System.out::println);
        //System.out.println(result.size());
    }
}
