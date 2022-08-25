package io.github.jsonflat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.github.jsonflat.schema.converter.Converter;
import com.jayway.jsonpath.JsonPath;
import io.github.jsonflat.schema.Schema;
import io.github.jsonflat.utils.CartesianProduct;
import io.github.jsonflat.utils.StringUtils;
import io.github.jsonflat.model.*;

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

public class FlatTransformer implements Serializable {
	Collection<Schema.Column> transformScheme;

	public FlatTransformer(Collection<Schema.Column> transformScheme) {
		this.transformScheme = transformScheme;
	}

	/**
	 * Transforms Json document to list of flat Json documents according to scheme
	 * @param document json to transformation
	 * @return list of flat Json documents
	 */

	public List<JsonNode> transform(JsonNode document) {
		List<ColumnResult> columnResults = new ArrayList<>();
		for (Schema.Column columnScheme : transformScheme) {
			try {
				ColumnResult c = eval(document, columnScheme, null);
				columnResults.add(c);
			} catch (StopTransformationRuntimeException ex) {
				columnResults.clear();
				break;
			}
		}
		List<List<Cell>> rowList = cartesian(columnResults);
		List<JsonNode> result = new ArrayList<>(rowList.size());
		for (List<Cell> list: rowList) {
			if (Cell.isNotEmptyRow(list)) {
				ObjectNode e = JsonNodeFactory.instance.objectNode(); //transform cell to JsonNode
				boolean addRowToResult = true;
				for (Cell v : list) {
					if (!v.writeToNode(e)) {
						addRowToResult = false;
						break;
					}
				}
				if (addRowToResult) {
					result.add(e);
				}
			}
		}
		return result;
	}

	private ColumnResult eval(JsonNode element, Schema.Column columnScheme, String parentName) {
		final ColumnResult result = new ColumnResult();
		result.setName(columnScheme.getFullname(parentName));
		String path = StringUtils.isNotBlank(columnScheme.getPath()) ?
						columnScheme.getPath()
						: columnScheme.getName();
		ArrayNode jsonPathValues = (element != null && path != null) ?
						JsonPath.read(element, path)
						: null;
		if (jsonPathValues == null || jsonPathValues.size() == 0) {
			if (columnScheme.isSkipJsonIfEmpty()) {
				throw new StopTransformationRuntimeException();
			}
			if (columnScheme.getColumns().size() == 0) {
				result.setValues(Collections.singletonList(new NullValue(columnScheme.isSkipRowIfEmpty())));
			} else {
				List<ColumnResult> subColumnResults = new ArrayList<>(columnScheme.getColumns().size());
				for (Schema.Column subColumnScheme : columnScheme.getColumns()) {
					ColumnResult c = eval(null, subColumnScheme, result.getName());
					subColumnResults.add(c);
				}
				result.setValues(toCompositeValues(subColumnResults));
				return result;
			}
			return result;
		}
		Collection<JsonNode> jpathResult = new ArrayList<>(jsonPathValues.size());
		jsonPathValues.forEach(jpathResult::add);
		//Processing leaf of json tree
		if (columnScheme.getColumns().isEmpty()) {
			result.getValues().addAll(
					processLeafs(jpathResult, columnScheme, result.getName())
			);
		} else { //processing middle nodes
			if (columnScheme.getGroup() == Schema.GroupPolicy.COLUMNS) {
				List<ColumnResult> subColumnResults = new ArrayList<>(columnScheme.getColumns().size()); //if group by columns - create common list of sub columns
				int i = 0;
				for (JsonNode subElement : jpathResult) {
					String resultColumnName = result.getName() + columnScheme.getSchema().getDelimiter() + i;
					boolean resultProcessed = false;
					for (Schema.Column subColumnScheme : columnScheme.getColumns()) {
						ColumnResult c = eval(subElement, subColumnScheme, resultColumnName);
						if (!c.isEmpty()) {
							subColumnResults.add(c);
							resultProcessed = true;
						}
					}
					if (!resultProcessed) { //Filter object nodes (if it's not processed, then it's not defined in schema)
						if (!subElement.isObject()) {
							subColumnResults.add(
									new ColumnResult(resultColumnName,
											processLeafs(Collections.singletonList(subElement), columnScheme, resultColumnName)
									)
							);
						}
					}
					i++;
				}
			}

			List<Value> compositeValues = new ArrayList<>();
			List<ColumnResult> subColumnResults = new ArrayList<>(columnScheme.getColumns().size()); //if group by columns - create common list of sub columns
			int i = 0;
			for (JsonNode subElement : jpathResult) {
				String resultColumnName = columnScheme.getGroup() == Schema.GroupPolicy.COLUMNS ?
						result.getName() + columnScheme.getSchema().getDelimiter() + i
						: result.getName();
				if (columnScheme.getGroup() != Schema.GroupPolicy.COLUMNS) {
					subColumnResults = new ArrayList<>(columnScheme.getColumns().size()); //if group by columns - create list of sub columns for each element
				}
				boolean resultProcessed = false;
				for (Schema.Column subColumnScheme : columnScheme.getColumns()) {
					ColumnResult c = eval(subElement, subColumnScheme, resultColumnName);
					if (!c.isEmpty()) {
						subColumnResults.add(c);
						resultProcessed = true;
					}
				}
				if (!resultProcessed) { //Filter object nodes (if it's not processed, then it's not defined in schema)
					if (!subElement.isObject()) {
						if (columnScheme.getGroup() != Schema.GroupPolicy.COLUMNS) {
							 compositeValues.addAll(
									processLeafs(Collections.singletonList(subElement), columnScheme, resultColumnName)
							);
						} else {
							subColumnResults.add(
									new ColumnResult(resultColumnName,
											processLeafs(Collections.singletonList(subElement), columnScheme, resultColumnName)
									)
							);
						}
					}
				} else if (columnScheme.getGroup() != Schema.GroupPolicy.COLUMNS) {
					//if group policy is not COLUMNS add result for each element
					compositeValues.addAll(toCompositeValues(subColumnResults));
				}
				i++;
			}
			if (columnScheme.getGroup() == Schema.GroupPolicy.COLUMNS) {
				compositeValues.addAll(toCompositeValues(subColumnResults));
			}
			result.getValues().addAll(compositeValues);
		}
		return result;
	}

	private List<Value> processLeafs(Collection<JsonNode> jsonPathValues, Schema.Column columnScheme, String name) {
		Converter converter = columnScheme.getConverter();
		List<JsonNode> convertedNodes = jsonPathValues.stream()
				.map(converter::convert)
				.collect(Collectors.toList());
		switch (columnScheme.getGroup()) {
			case ARRAY:
				if (convertedNodes.size() == 1)
					return Collections.singletonList(new JsonValue(convertedNodes.get(0), columnScheme.isSkipRowIfEmpty()));
				ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode();
				convertedNodes.forEach(arrayNode::add);
				return Collections.singletonList(new JsonValue(arrayNode, columnScheme.isSkipRowIfEmpty()));
			case CONCAT:
				if (convertedNodes.size() == 1)
					return Collections.singletonList(new JsonValue(convertedNodes.get(0), columnScheme.isSkipRowIfEmpty()));
				return Collections.singletonList(
						new JsonValue(
								new TextNode(convertedNodes.stream().map(JsonNode::asText).collect(Collectors.joining(Schema.GROUP))),
								columnScheme.isSkipRowIfEmpty()
						)
				);
			case COLUMNS:
				List<Cell> cells = new ArrayList<>(convertedNodes.size());
				for (int i = 0; i < convertedNodes.size(); i++) {
					cells.add(
							new Cell(
									name + columnScheme.getSchema().getDelimiter() + i,
									new JsonValue(convertedNodes.get(i), columnScheme.isSkipRowIfEmpty())
							)
					);
				}
				return Collections.singletonList(new CompositeValue(cells));
			case NO_GROUP:
				if (convertedNodes.size() == 1)
					return Collections.singletonList(new JsonValue(convertedNodes.get(0), columnScheme.isSkipRowIfEmpty()));
				return convertedNodes.stream()
						.map(j -> new JsonValue(j, columnScheme.isSkipRowIfEmpty()))
						.collect(Collectors.toList());
		}
		return Collections.emptyList();
	}

	private List<List<Cell>> cartesian(List<ColumnResult> subcolumns) {
		return CartesianProduct.cartesianProduct(
				subcolumns.stream()
						.map(ColumnResult::toCells)
						.collect(Collectors.toList())
		);
	}

	private List<Value> toCompositeValues(List<ColumnResult> subcolumns) {
		List<List<Cell>> rowList = cartesian(subcolumns);
		List<Value> compositeValues = new ArrayList<>(rowList.size());
		if (rowList.size() > 0) {
			int rowLength = rowList.get(0).size();
			for (List<Cell> cell : rowList) {
				List<Cell> cellColumnValues = new ArrayList<>(rowLength);
				for (int i = 0; i < rowLength; i++) {
					cellColumnValues.add(cell.get(i));
				}
				compositeValues.add(new CompositeValue(cellColumnValues));
			}
		}
		return compositeValues;
	}

	private static class StopTransformationRuntimeException extends RuntimeException {
	}
}
