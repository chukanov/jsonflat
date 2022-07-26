package io.github.chukanov.jflat;

import io.github.chukanov.jflat.schema.AutoSchemaFactory;
import io.github.chukanov.jflat.schema.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

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

public class SchemaTest {
  @Test
  public void getNamesTest() throws IOException {
    String jsonText = "{\n" +
      "  \"one\":1,\n" +
      "  \"two\":[\n" +
      "    {\"twenty\": 406015200000,\"twenty1\": [{\"twenty11\": 2011},{\"twenty11\": 2012}]},\n" +
      "    {\"twenty\": 383810400000},\n" +
      "    {\"twenty\": 1286406000000}\n" +
      "  ],\n" +
      "  \"three\": 3\n" +
      "}";
    Schema schema = AutoSchemaFactory.builder()
      .build()
      .generate(jsonText);

    Assert.assertEquals(schema.getResultNames(),
      Arrays.asList(
        "one","two_twenty","two_twenty1_twenty11","three"
      )
    );
  }
}
