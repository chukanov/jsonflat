package io.github.chukanov.jflat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.github.chukanov.jflat.schema.converter.Converter;
import com.jayway.jsonpath.JsonPath;
import io.github.chukanov.jflat.schema.Schema;
import io.github.chukanov.jflat.utils.CartesianProduct;
import io.github.chukanov.jflat.utils.StringUtils;
import io.github.chukanov.jflat.model.*;

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

	public List<JsonNode> transform(JsonNode document) {
		List<Column> columns = new ArrayList<>();
		for (Schema.Column columnScheme : transformScheme) {
			try {
				Column c = eval(document, columnScheme, null);
				columns.add(c);
			} catch (StopRecursionException ex) {
				columns.clear();
				break;
			}
		}
		List<List<RowValue>> rowList = cartesian(columns);
		List<JsonNode> result = new ArrayList<>(rowList.size());
		rowList.stream()
				//filter null-only rows
				.filter(
						list ->
								list.stream().map(RowValue::getCell).filter(Cell::isEmpty).count() != list.size()
				).forEach(
						//transform row to JsonNode
						row -> {
							ObjectNode e = JsonNodeFactory.instance.objectNode();
							boolean addRowToResult = true;
							for (RowValue v : row) {
								if (v.getCell() instanceof CompositeCell) {
									for (RowValue nv : (CompositeCell) v.getCell()) {
										addRowToResult = applyRowValue(nv, e);
										if (!addRowToResult) {
											break;
										}
									}
								} else {
									addRowToResult = applyRowValue(v, e);
								}
								if (!addRowToResult) {
									break;
								}
							}
							if (addRowToResult) {
								result.add(e);
							}
						}
				);
		return result;
	}

	private boolean applyRowValue(RowValue v, ObjectNode node) {
		Cell cell = v.getCell();
		if (cell.isEmpty()) {
			return !cell.isRequired();
		} else {
			JsonNode value = ((JsonCell) cell).getValue();
			node.set(v.getName(), value);
		}
		return true;
	}

	private List<Cell> processLeafs(Collection<JsonNode> jsonPathValues, Schema.Column columnScheme, String name) {
		Converter converter = columnScheme.getConverter();
		List<JsonNode> convertedNodes = jsonPathValues.stream()
				.map(converter::convert)
				.collect(Collectors.toList());
		switch (columnScheme.getGroup()) {
			case ARRAY:
				if (convertedNodes.size() == 1)
					return Collections.singletonList(new JsonCell(convertedNodes.get(0), columnScheme.isSkipRowIfEmpty()));
				ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode();
				convertedNodes.forEach(arrayNode::add);
				return Collections.singletonList(new JsonCell(arrayNode, columnScheme.isSkipRowIfEmpty()));
			case CONCAT:
				if (convertedNodes.size() == 1)
					return Collections.singletonList(new JsonCell(convertedNodes.get(0), columnScheme.isSkipRowIfEmpty()));
				return Collections.singletonList(
						new JsonCell(
								new TextNode(convertedNodes.stream().map(JsonNode::asText).collect(Collectors.joining(Schema.GROUP))),
								columnScheme.isSkipRowIfEmpty()
						)
				);
			case COLUMNS:
				List<RowValue> rowValues = new ArrayList<>(convertedNodes.size());
				for (int i = 0; i < convertedNodes.size(); i++) {
					rowValues.add(
							new RowValue(
									name + columnScheme.getSchema().getDelimiter() + i,
									new JsonCell(convertedNodes.get(i), columnScheme.isSkipRowIfEmpty())
							)
					);
				}
				return Collections.singletonList(new CompositeCell(rowValues));
			case NO_GROUP:
				if (convertedNodes.size() == 1)
					return Collections.singletonList(new JsonCell(convertedNodes.get(0), columnScheme.isSkipRowIfEmpty()));
				return convertedNodes.stream()
						.map(j -> new JsonCell(j, columnScheme.isSkipRowIfEmpty()))
						.collect(Collectors.toList());
		}
		return Collections.emptyList();
	}

	private Column eval(JsonNode element, Schema.Column columnScheme, String parentName) {
		final Column result = new Column();
		result.setName(columnScheme.getFullname(parentName));
		String path =
				StringUtils.isNotBlank(columnScheme.getPath()) ?
						columnScheme.getPath()
						: columnScheme.getName();
		ArrayNode jsonPathValues =
				(element != null && path != null) ?
						JsonPath.read(element, path) :
						null;
		if (jsonPathValues == null || jsonPathValues.size() == 0) {
			if (columnScheme.isSkipJsonIfEmpty()) {
				throw new StopRecursionException();
			}
			if (columnScheme.getColumns().size() == 0) {
				result.setCells(Collections.singletonList(new NullCell(columnScheme.isSkipRowIfEmpty())));
			} else {
				List<Column> subColumns = new ArrayList<>(columnScheme.getColumns().size());
				for (Schema.Column subColumnScheme : columnScheme.getColumns()) {
					Column c = eval(null, subColumnScheme, result.getName());
					subColumns.add(c);
				}
				result.setCells(toCompositeCells(subColumns));
				return result;
			}
			return result;
		}
		Collection<JsonNode> jpathResult = new ArrayList<>(jsonPathValues.size());
		jsonPathValues.forEach(jpathResult::add);
		//Processing leaf of json tree
		if (columnScheme.getColumns().isEmpty()) {
			result.getCells().addAll(
					processLeafs(jpathResult, columnScheme, result.getName())
			);
		} else { //processing middle nodes
			List<Cell> compositeCells = new ArrayList<>();
			List<Column> subColumns = new ArrayList<>(columnScheme.getColumns().size()); //if group by columns - create common list of sub columns
			int i = 0;
			for (JsonNode subElement : jpathResult) {
				String resultColumnName = columnScheme.getGroup() == Schema.GroupPolicy.COLUMNS ?
						result.getName() + columnScheme.getSchema().getDelimiter() + i
						: result.getName();
				if (columnScheme.getGroup() != Schema.GroupPolicy.COLUMNS) {
					subColumns = new ArrayList<>(columnScheme.getColumns().size()); //if group by columns - create list of sub columns for each element
				}
				boolean resultProcessed = false;
				for (Schema.Column subColumnScheme : columnScheme.getColumns()) {
					Column c = eval(subElement, subColumnScheme, resultColumnName);
					if (!c.isEmpty() || subColumnScheme.isSkipRowIfEmpty()) {
						subColumns.add(c);
						resultProcessed = true;
					}
				}
				if (!resultProcessed) { //Filter object nodes (if it's not processed, than it's not defined in schema)
					if (!subElement.isObject()) {
						if (columnScheme.getGroup() != Schema.GroupPolicy.COLUMNS) {
							compositeCells.addAll(
									processLeafs(Collections.singletonList(subElement), columnScheme, resultColumnName)
							);
						} else {
							subColumns.add(
									new Column(resultColumnName,
											processLeafs(Collections.singletonList(subElement), columnScheme, resultColumnName)
									)
							);
						}
					}
				} else if (columnScheme.getGroup() != Schema.GroupPolicy.COLUMNS) {
					//if group policy is not COLUMNS add result for each element
					compositeCells.addAll(toCompositeCells(subColumns));
				}
				i++;
			}
			if (columnScheme.getGroup() == Schema.GroupPolicy.COLUMNS) {
				compositeCells.addAll(toCompositeCells(subColumns));
			}
			result.getCells().addAll(compositeCells);
		}
		return result;
	}

	private List<List<RowValue>> cartesian(List<Column> subcolumns) {
		return CartesianProduct.cartesianProduct(
				subcolumns.stream()
						.map(Column::toRowValues)
						.collect(Collectors.toList())
		);
	}

	private List<Cell> toCompositeCells(List<Column> subcolumns) {
		List<List<RowValue>> rowList = cartesian(subcolumns);
		List<Cell> compositeCells = new ArrayList<>(rowList.size());
		if (rowList.size() > 0) {
			int rowLength = rowList.get(0).size();
			for (List<RowValue> row : rowList) {
				List<RowValue> rowColumnValues = new ArrayList<>(rowLength);
				for (int i = 0; i < rowLength; i++) {
					rowColumnValues.add(row.get(i));
				}
				compositeCells.add(new CompositeCell(rowColumnValues));
			}
		}
		return compositeCells;
	}

	private static class StopRecursionException extends RuntimeException {
	}
}
