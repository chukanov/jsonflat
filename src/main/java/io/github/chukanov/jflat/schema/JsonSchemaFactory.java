package io.github.chukanov.jflat.schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Builder;

import java.io.IOException;
import java.util.Collection;

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
 *
 * @author Evgeniy Chukanov
 */

@Builder
public class JsonSchemaFactory {

	public Schema generate(String schemaJsonString) throws IOException {
		ObjectMapper MAPPER = new ObjectMapper();
		Schema schema = MAPPER.readValue(schemaJsonString, Schema.class);

		//workaround setting scheme for each column
		//cause jackson doesn't support object inner class
		redefineSchema(schema.getColumns(), schema);
		return schema;
	}

	private void redefineSchema(Collection<Schema.Column> columns, Schema schema) {
		for (Schema.Column c : columns) {
			c.setSchema(schema);
			if (!c.getColumns().isEmpty()) {
				redefineSchema(c.getColumns(), schema);
			}
		}
	}
}
