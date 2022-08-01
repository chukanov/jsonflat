package io.github.jsonflat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;
import io.github.jsonflat.schema.Schema;
import io.github.jsonflat.utils.StringUtils;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

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

public class Transformer implements Serializable {
	public static final ObjectMapper MAPPER = new ObjectMapper();

	static {
		initJsonPath();
	}

	private final FlatTransformer transformer;
	private final Schema schema;

	public Transformer(Schema schema) {
		this.schema = schema;
		this.transformer = new FlatTransformer(schema.getColumns());
	}

	public Schema getSchema() {
		return schema;
	}

	public List<String> transform(String jsonData) throws IOException {
		if (StringUtils.isBlank(jsonData)) return Collections.emptyList();
		JsonNode json = MAPPER.readTree(jsonData);
		if (json.isArray()) {
			List<JsonNode> jsonNodes = new ArrayList<>(json.size());
			json.forEach(jsonNodes::add);
			return jsonNodes.stream()
					.flatMap(j -> this.transform(j).stream())
					.map(JsonNode::toString)
					.collect(Collectors.toList());
		} else {
			return this.transform(json).stream()
					.map(JsonNode::toString)
					.collect(Collectors.toList());
		}
	}

	public List<JsonNode> transform(JsonNode json) {
		if (json == null) return Collections.emptyList();
		if (schema.getFilter().apply(json)) {
			return transformer.transform(json);
		}
		return Collections.emptyList();
	}

	private static void initJsonPath() {
		Configuration.setDefaults(new Configuration.Defaults() {
			private final JsonProvider jsonProvider = new JacksonJsonNodeJsonProvider();
			private final MappingProvider mappingProvider = new JacksonMappingProvider();

			@Override
			public JsonProvider jsonProvider() {
				return jsonProvider;
			}

			@Override
			public MappingProvider mappingProvider() {
				return mappingProvider;
			}

			@Override
			public Set<Option> options() {
				return EnumSet.of(Option.ALWAYS_RETURN_LIST, Option.SUPPRESS_EXCEPTIONS);
			}
		});

	}
}
