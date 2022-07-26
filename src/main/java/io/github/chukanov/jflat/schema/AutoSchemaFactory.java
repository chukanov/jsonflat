package io.github.chukanov.jflat.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Builder;
import lombok.val;
import io.github.chukanov.jflat.schema.filter.Exist;
import io.github.chukanov.jflat.schema.filter.Filter;
import io.github.chukanov.jflat.utils.JsonUtils;
import io.github.chukanov.jflat.utils.StringUtils;

import java.io.IOException;
import java.util.*;

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
public class AutoSchemaFactory {
	@Builder.Default
	List<String> columnStringFilters = Collections.emptyList();
	@Builder.Default
	Object[] columnArrayFilters = {};
	@Builder.Default
	String filterRowsPath = null;
	@Builder.Default
	String delimiter = Schema.DEFAULT_DELIMITER;
	@Builder.Default
	Schema.GroupPolicy primitiveArraysGroup = Schema.GroupPolicy.ARRAY;
	@Builder.Default
	Schema.GroupPolicy complexArraysGroup = Schema.GroupPolicy.NO_GROUP;
	boolean processMixedArrays;

	public Schema generate(String jsonText) throws IOException {
		ObjectMapper MAPPER = new ObjectMapper();
		if (StringUtils.isBlank(jsonText)) return new Schema();
		return generate(MAPPER.readValue(jsonText, JsonNode.class));
	}

	public Schema generate(JsonNode document) {
		val schema = new Schema(
				null,
				null,
				StringUtils.isNotBlank(filterRowsPath) ? new Exist(filterRowsPath) : Filter.DEFAULT,
				new ArrayList<>(),
				delimiter
		);
		val column = new Schema.Column("", "", schema);
		processSubNodes(document, column);
		schema.setColumns(column.getColumns());
		val schemaFiltered = schema.filterColumns(columnStringFilters);
		return schemaFiltered.filterColumns(columnArrayFilters);
	}


	private void reformatUnnamedArrays(ArrayNode node) {
		val elements = node.elements();
		int i = 0;
		while (elements.hasNext()) {
			JsonNode e = elements.next();
			if (!e.isObject()) {
				ObjectNode obj = JsonNodeFactory.instance.objectNode();
				obj.set(String.valueOf(i), e);
				node.set(i, obj);
			}
			i++;
		}
	}

	private void processSubNodes(JsonNode node, Schema.Column root) {
		if (node.isObject()) {
			val nameSet = JsonUtils.names(node);
			for (String name : nameSet) {
				val subnode = node.get(name);
				produceColumn(subnode, root, name);
			}
		} else if (node.isArray()) {
			produceColumn(node, root, root.getPath());
		}
		root.setColumns(new ArrayList<>(new LinkedHashSet<>(root.getColumns())));
	}

	private void produceColumn(JsonNode subnode, Schema.Column root, String name) {
		if (subnode.isArray()) {
			Schema.Column column;
			if (StringUtils.isNotBlank(name)) {
				column = new Schema.Column(name, name + "[*]", root.getSchema());
				root.getColumns().add(column);
			} else {
				column = root;
			}
			Iterator<JsonNode> elements = subnode.elements();
			while (elements.hasNext()) {
				JsonNode e = elements.next();
				if (e.isObject()) {
					processSubNodes(e, column);
				}
			}
			if (column.getColumns().isEmpty()) {
				column.setGroup(primitiveArraysGroup);
			} else {
				column.setGroup(complexArraysGroup);
			}
		} else {
			val column = new Schema.Column(name, name, root.getSchema());
			root.getColumns().add(column);
			processSubNodes(subnode, column);
		}
	}
}
