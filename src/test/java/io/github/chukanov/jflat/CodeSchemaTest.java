package io.github.chukanov.jflat;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.val;
import org.junit.Test;
import io.github.chukanov.jflat.schema.Schema;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import static org.junit.Assert.assertEquals;

import static io.github.chukanov.jflat.Transformer.MAPPER;
/*
    schema.setColumns(
      Arrays.asList(
        new Schema.Column("one","one", Collections.singletonList(
          new Schema.Column("sub", "sub[*]",
            Arrays.asList(
              new Schema.Column("foo", "foo", Collections.emptyList(), schema),
              new Schema.Column("3", "[3]",
                Collections.emptyList()
                Arrays.asList(
                  new Schema.Column("0", "[0]", Collections.emptyList(), schema),
                  new Schema.Column("1", "[1]", Arrays.asList(
                    new Schema.Column("0", "[0]", Collections.emptyList(), schema),
                    new Schema.Column("1", "[1]", Collections.emptyList(), schema)
                  ), schema),
                  new Schema.Column("2", "[2]", Collections.emptyList(), schema),
                  new Schema.Column("3", "[3]", Arrays.asList(
                    new Schema.Column("a", "a", Collections.emptyList(), schema),
                    new Schema.Column("b", "b", Collections.emptyList(), schema)
                  ), schema)
                )
                , schema)
            ),
            Schema.GroupPolicy.NO_GROUP, schema)
        ), schema),
        new Schema.Column("two", "two", Collections.emptyList(), schema)
      )
    );
 */
public class CodeSchemaTest {
  @Test
  public void testArray() throws IOException {
    val js ="{\n" +
      "  \"one\": {\"sub\": [1,true,{\"foo\":1}, [5,[51,52],6,[{\"a\":1},{\"b\":2}]],{\"bar\":[3,4]},\"xxx\"]},\n" +
      "  \"two\": 2\n" +
      "}";
    val node = MAPPER.readValue(js, JsonNode.class);
    Schema schema = new Schema();
    schema.setColumns(
      Arrays.asList(
        new Schema.Column("one","one", Arrays.asList(
          new Schema.Column("sub0", "sub[0]",
            Collections.emptyList(),
            Schema.GroupPolicy.NO_GROUP, schema
          ),
          new Schema.Column("sub1", "sub[1]",
            Collections.emptyList(),
            Schema.GroupPolicy.NO_GROUP, schema
          ),
          new Schema.Column("sub2", "sub[2]",
            Collections.emptyList(),
            Schema.GroupPolicy.NO_GROUP, schema
          )
        ), schema),
        new Schema.Column("two", "two", Collections.emptyList(), schema)
      )
    );
    Transformer transformer = new Transformer(schema);
    val result = transformer.transform(js);
    result.forEach(System.out::println);
    /*assertEquals(
      Arrays.asList(
        "{\"one_sub\":1,\"two\":2}",
        "{\"one_sub\":true,\"two\":2}",
        "{\"one_sub_foo\":1,\"two\":2}",
        "{\"one_sub_0\":5,\"one_sub_1_0\":51,\"one_sub_1_1\":52,\"one_sub_2\":6,\"one_sub_3_a\":1,\"two\":2}",
        "{\"one_sub_0\":5,\"one_sub_1_0\":51,\"one_sub_1_1\":52,\"one_sub_2\":6,\"one_sub_3_b\":2,\"two\":2}",
        "{\"one_sub\":\"xxx\",\"two\":2}"
      ),
      result
    );*/
  }
}
