package io.github.jsonflat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.jsonflat.utils.JsonUtils;
import lombok.val;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

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

public class JsonUtilsTest {
  @Test
  public void csv() throws IOException {
    String line =  "{\"one\":\"1\",\"two_twenty\":21,\"three\":\"3 \\\"text\\\" \", \"arr\":[0,1,2]}";
    ObjectNode json = (ObjectNode) MAPPER.readTree(line);
    String header = JsonUtils.writeCsvHeader(
      Arrays.asList("one", "two_twenty", "three", "arr"),";"
    );
    assertEquals("one;two_twenty;three;arr", header);

    String value = JsonUtils.writeCsvValue(
      Arrays.asList("one", "two_twenty", "three", "arr"),json,";"
    );
    assertEquals("\"1\";21;\"3 \\\"text\\\" \";\"[0,1,2]\"", value);
  }

  @Test
  public void unnamedArraysFire() throws IOException {
    val js ="{\n" +
      "  \"one\": {\"sub1\": [1,true,{\"foo\":1}, [5,[51,52],6,[{\"a\":1},{\"b\":2}]],{\"bar\":[3,4]}]},\n" +
      "  \"two\": 2\n" +
      "}";
    val node = MAPPER.readValue(js, JsonNode.class);
    val res = JsonUtils.reformatUnnamedArrays(node);
    assertEquals("{\"one\":{\"sub1\":[1,true,{\"foo\":1},[{\"0\":5},{\"1\":[51,52]},{\"2\":6},{\"3\":[{\"a\":1},{\"b\":2}]}],{\"bar\":[3,4]}]},\"two\":2}", res.toString());
  }

  @Test
  public void unnamedArraysFullFire() throws IOException {
    val js ="[[11,12,13],[21,22],[[3],[4]]]";
    val node = MAPPER.readValue(js, JsonNode.class);
    val res = JsonUtils.reformatUnnamedArrays(node);
    //System.out.println(res);
  }

}
