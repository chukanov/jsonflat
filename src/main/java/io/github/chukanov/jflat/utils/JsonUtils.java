package io.github.chukanov.jflat.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.val;

import java.util.*;
import java.util.function.Predicate;
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

public class JsonUtils {

	public static List<JsonNode> select(JsonNode node, Predicate<JsonNode> predicate) {
		if (node == null) return Collections.emptyList();
		List<JsonNode> result = new ArrayList<>(node.size());
		node.forEach(
				n -> {
					if (predicate.test(n)) result.add(n);
				}
		);
		return result;
	}

	public static boolean anyChild(JsonNode node, Predicate<JsonNode> predicate) {
		if (node == null) return false;
		Iterator<JsonNode> it = node.elements();
		while (it.hasNext()) {
			if (predicate.test(it.next())) return true;
		}
		return false;
	}

	public static Set<String> names(JsonNode node) {
		val nameIterator = node.fieldNames();
		val nameSet = new LinkedHashSet<String>();
		while (nameIterator.hasNext()) {
			nameSet.add(nameIterator.next());
		}
		return nameSet;
	}


	public static String writeCsvHeader(List<String> names, String delimiter) {
		return String.join(delimiter, names);
	}

	public static String writeCsvValue(List<String> names, JsonNode node, String delimiter) {
		return names.stream()
				.map(n -> getCSVValue(node.get(n)))
				.collect(Collectors.joining(delimiter));
	}

	private static String getCSVValue(JsonNode node) {
		if (node == null || node.isNull()) return "";
		if (node.isBinary() || node.isNumber()) return node.asText();
		if (node.isArray()) return "\"" + StringUtils.escapeQuotes(node.toString()) + "\"";
		else return "\"" + StringUtils.escapeQuotes(node.asText()) + "\"";
	}

	public static void reformatNestedUnnamedArrays(JsonNode node) {
		if (node.isArray()) {
			val array = (ArrayNode) node;
			List<JsonNode> nestedArrays = select(array, JsonNode::isArray);
			for (JsonNode subNode : nestedArrays) {
				List<JsonNode> subNestedArrays = select(subNode, JsonNode::isArray);
				if (subNestedArrays.size() > 0) {
					ArrayNode subArray = (ArrayNode) subNode;
					val elements = subNode.elements();
					int i = 0;
					while (elements.hasNext()) {
						JsonNode e = elements.next();
						if (!e.isObject()) {
							ObjectNode obj = JsonNodeFactory.instance.objectNode();
							obj.set(String.valueOf(i), e);
							subArray.set(i, obj);
						}
						i++;
					}
				}
			}
		}
		val elements = node.elements();
		while (elements.hasNext()) {
			reformatNestedUnnamedArrays(elements.next());
		}
	}


	public static JsonNode reformatUnnamedArrays(JsonNode node) {
		JsonNode copy = node.deepCopy();
		if (node.isArray()) {
			ObjectNode obj = JsonNodeFactory.instance.objectNode();
			reformatNestedUnnamedArrays(copy);
			obj.set("0", copy);
			return obj;
		} else {
			reformatNestedUnnamedArrays(copy);
			return copy;
		}
	}
}
