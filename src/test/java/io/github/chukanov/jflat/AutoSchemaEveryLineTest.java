package io.github.chukanov.jflat;

import org.junit.Test;
import io.github.chukanov.jflat.schema.AutoSchemaFactory;
import io.github.chukanov.jflat.schema.Schema;

import java.io.IOException;
import java.util.Arrays;
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

public class AutoSchemaEveryLineTest {
    @Test
    public void testFilterOneTwo() throws IOException {
        String jsonText1 = "{\"two\":[{\"twenty1\":121},{\"twenty2\":122},{\"twenty3\":123}],\"three\": 13}";
        String jsonText2 = "{\"one\":21,\"two\":[{\"twenty1\":221},{\"twenty2\":222},{\"twenty3\":223}]}";
        Schema schema1 = AutoSchemaFactory.builder()
                .build()
                .generate(jsonText1);
        Schema schema2 = AutoSchemaFactory.builder()
          .build()
          .generate(jsonText2);
        schema1.merge(schema2);

        Transformer transformer = new Transformer(schema1);
        List<String> result = transformer.transform(jsonText1);
        result.addAll(transformer.transform(jsonText2));
        assertEquals(
          Arrays.asList(
            "{\"two_twenty1\":121,\"three\":13}",
            "{\"two_twenty2\":122,\"three\":13}",
            "{\"two_twenty3\":123,\"three\":13}",
            "{\"two_twenty1\":221,\"one\":21}",
            "{\"two_twenty2\":222,\"one\":21}",
            "{\"two_twenty3\":223,\"one\":21}"
          ), result
        );
    }

    @Test
    public void testFilterOneTwo2() throws IOException {
        String jsonText1 = "{\"two\":[{\"twenty3\":123}],\"three\": 13}";
        String jsonText2 = "{\"one\":21,\"two\":[{\"twenty1\":221},{\"twenty2\":222},{\"twenty3\":223}]}";
        Schema schema1 = AutoSchemaFactory.builder()
          .build()
          .generate(jsonText1);
        Schema schema2 = AutoSchemaFactory.builder()
          .build()
          .generate(jsonText2);
        schema1.merge(schema2);

        Transformer transformer = new Transformer(schema1);
        List<String> result = transformer.transform(jsonText1);
        result.addAll(transformer.transform(jsonText2));
        assertEquals(
          Arrays.asList(
            "{\"two_twenty3\":123,\"three\":13}",
            "{\"two_twenty1\":221,\"one\":21}",
            "{\"two_twenty2\":222,\"one\":21}",
            "{\"two_twenty3\":223,\"one\":21}"
          ), result
        );
    }
    @Test
    public void testMergeSchema() throws IOException {
        String json1 = "{\"application\":{\"name\":\"app\",\"operation-fail\":false,\"properties\":[{\"name\":\"A\",\"granted\":true},{\"name\":\"B\",\"granted\":true},{\"name\":\"C\",\"granted\":true,\"cancelled\":true}]}}";
        String json2 = "{\"application\":{\"name\":\"app\",\"operation-success\":true,\"properties\":[{\"name\":\"A\"},{\"name\":\"B\",\"granted\":false},{\"name\":\"C\",\"granted\":true},{\"name\":\"D\",\"granted\":true},{\"name\":\"E\",\"granted\":true}],\"metadata\":{\"time\":1589360405806,\"timezone\":\"utc\",\"request_id\":\"8d7a3728209\"},\"enableArchive\":true}}";
        Schema schema1 = AutoSchemaFactory.builder()
          .build()
          .generate(json1);
        Schema schema2 = AutoSchemaFactory.builder()
          .build()
          .generate(json2);
        schema1.merge(schema2);

        Transformer transformer = new Transformer(schema1);
        List<String> result = transformer.transform(json1);
        result.addAll(transformer.transform(json2));
        assertEquals(
          Arrays.asList(
            "{\"application_name\":\"app\",\"application_operation-fail\":false,\"application_properties_name\":\"A\",\"application_properties_granted\":true}",
            "{\"application_name\":\"app\",\"application_operation-fail\":false,\"application_properties_name\":\"B\",\"application_properties_granted\":true}",
            "{\"application_name\":\"app\",\"application_operation-fail\":false,\"application_properties_name\":\"C\",\"application_properties_granted\":true,\"application_properties_cancelled\":true}",
            "{\"application_name\":\"app\",\"application_properties_name\":\"A\",\"application_operation-success\":true,\"application_metadata_time\":1589360405806,\"application_metadata_timezone\":\"utc\",\"application_metadata_request_id\":\"8d7a3728209\",\"application_enableArchive\":true}",
            "{\"application_name\":\"app\",\"application_properties_name\":\"B\",\"application_properties_granted\":false,\"application_operation-success\":true,\"application_metadata_time\":1589360405806,\"application_metadata_timezone\":\"utc\",\"application_metadata_request_id\":\"8d7a3728209\",\"application_enableArchive\":true}",
            "{\"application_name\":\"app\",\"application_properties_name\":\"C\",\"application_properties_granted\":true,\"application_operation-success\":true,\"application_metadata_time\":1589360405806,\"application_metadata_timezone\":\"utc\",\"application_metadata_request_id\":\"8d7a3728209\",\"application_enableArchive\":true}",
            "{\"application_name\":\"app\",\"application_properties_name\":\"D\",\"application_properties_granted\":true,\"application_operation-success\":true,\"application_metadata_time\":1589360405806,\"application_metadata_timezone\":\"utc\",\"application_metadata_request_id\":\"8d7a3728209\",\"application_enableArchive\":true}",
            "{\"application_name\":\"app\",\"application_properties_name\":\"E\",\"application_properties_granted\":true,\"application_operation-success\":true,\"application_metadata_time\":1589360405806,\"application_metadata_timezone\":\"utc\",\"application_metadata_request_id\":\"8d7a3728209\",\"application_enableArchive\":true}"
          ), result
        );

    }

}
