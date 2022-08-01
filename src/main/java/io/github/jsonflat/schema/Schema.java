package io.github.jsonflat.schema;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.github.jsonflat.schema.converter.Converter;
import io.github.jsonflat.schema.filter.Filter;
import lombok.*;
import io.github.jsonflat.utils.StringUtils;

import java.io.Serializable;
import java.util.*;
import java.util.function.Function;
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

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Schema implements Serializable {
	public static final String DEFAULT_DELIMITER = "_";
	public static final String GROUP = ",";

	private String name;    //name of scheme (nullable)
	private String version; //version of scheme
	private Filter filter = Filter.DEFAULT; //filter input document
	private Collection<Column> columns = new ArrayList<>();
	private String delimiter = DEFAULT_DELIMITER;

	/**
	 * Filter by column names from result
	 * support wildcards "*" and "?"
	 *
	 * @param stringFilter list of column names from result
	 * @return filtered parser schema
	 */
	public Schema filterColumns(List<String> stringFilter) {
		if (stringFilter == null || stringFilter.size() == 0) {
			return this;
		}
		return new Schema(name, version, filter, filterColumns(stringFilter, columns, ""), delimiter);
	}

	/**
	 * Filter by column name paths
	 * element should be String or String[]
	 * same as json_normalize paths
	 * example:
	 * [ 'a', ['b','c'], ['d','e','f'], 'g']
	 * represent paths:
	 * $.a[*]
	 * $.b.c[*]
	 * $.d.e.f[*]
	 * $.g[*]
	 *
	 * @param columnsFilter array of json path
	 * @return filtered parser schema
	 */
	public Schema filterColumns(Object[] columnsFilter) {
		if (columnsFilter == null
				|| columnsFilter.length == 0
				|| Arrays.stream(columnsFilter).anyMatch(Objects::isNull)
				|| Arrays.stream(columnsFilter).map(Object::toString).anyMatch(StringUtils::isBlank)
		) {
			return this;
		}
		val stringFilter = new ArrayList<String>(columnsFilter.length);
		Arrays.stream(columnsFilter).forEach(
				e -> {
					if (e instanceof Object[]) {
						stringFilter.add(Arrays.stream((Object[]) e).map(Object::toString).collect(Collectors.joining(delimiter)) + "*");
					} else {
						stringFilter.add(e.toString() + "*");
					}
				}
		);
		return new Schema(name, version, filter, filterColumns(stringFilter, columns, ""), delimiter);
	}

	private Collection<Column> filterColumns(List<String> columnsFilters, Collection<Column> columns, String parent) {
		Collection<Column> filteredColumns = new LinkedHashSet<>(columns.size());
		for (Column c : columns) {
			if (columnsFilters.stream().anyMatch(f -> Objects.equals(f, c.getFullname(parent)))) {
				c.setGroup(GroupPolicy.ARRAY);
				c.getColumns().clear();
				filteredColumns.add(c);
			} else if (c.match(columnsFilters, parent)) {
				if (c.columns.size() > 0) {
					c.setColumns(
							filterColumns(columnsFilters, c.columns, c.getFullname(parent))
					);
				}
				filteredColumns.add(c);
			}
		}
		return filteredColumns;
	}

	public void merge(Schema that) {
		Column thisRoot = new Column(null, this.columns, this);
		Column thatRoot = new Column(null, that.columns, that);
		thisRoot.merge(thatRoot);
		this.setColumns(thisRoot.getColumns());
	}

	public List<String> getResultNames() {
		return new ArrayList<>(this.getResultNames(this.columns, ""));
	}

	private LinkedHashSet<String> getResultNames(Collection<Column> column, String parent) {
		LinkedHashSet<String> names = new LinkedHashSet<>();
		for (Column c : column) {
			String fullname = c.getFullname(parent);
			if (c.columns.isEmpty()) names.add(fullname);
			else names.addAll(getResultNames(c.columns, fullname));
		}
		return names;
	}

	@Override
	public String toString() {
		return "Schema{" +
				"name='" + name + '\'' +
				", version='" + version + '\'' +
				'}';
	}

	@NoArgsConstructor
	@Getter
	@Setter
	@ToString
	public static class Column implements Serializable {
		private String name;        //relative name of column in result
		private String path = "";   //path in source
		private boolean fullname = false; //use name as absolute name
		private boolean skipJsonIfEmpty = false;    //skip document parsing if true
		private boolean skipRowIfEmpty = false;     //skip result of parsing if true
		private Converter converter = Converter.DEFAULT;
		private Collection<Column> columns = new ArrayList<>();
		private GroupPolicy group = GroupPolicy.NO_GROUP;

		@JsonIgnore
		private Schema schema;

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			Column column = (Column) o;
			return Objects.equals(name, column.name);
		}

		@Override
		public int hashCode() {
			return Objects.hash(name);
		}

		Column(String name, String path, Schema schema) {
			this.name = name;
			this.path = path;
			this.schema = schema;
		}

		public Column(String path, Collection<Column> columns, Schema schema) {
			this.path = path;
			this.columns = columns;
			this.schema = schema;
		}

		public Column(String name, String path, Collection<Column> columns, Schema schema) {
			this.name = name;
			this.path = path;
			this.columns = columns;
			this.schema = schema;
		}

		public Column(String name, String path, Collection<Column> columns, GroupPolicy groupPolicy, Schema schema) {
			this.name = name;
			this.path = path;
			this.columns = columns;
			this.schema = schema;
			this.group = groupPolicy;
		}

		public Column(String name, String path, Collection<Column> columns, GroupPolicy group, boolean fullname, boolean skipJsonIfEmpty, boolean skipRowIfEmpty, Converter converter, Schema schema) {
			this.name = name;
			this.path = path;
			this.fullname = fullname;
			this.skipJsonIfEmpty = skipJsonIfEmpty;
			this.skipRowIfEmpty = skipRowIfEmpty;
			this.converter = converter;
			this.columns = columns;
			this.group = group;
			this.schema = schema;
		}

		public String getFullname(String parent) {
			if (StringUtils.isBlank(parent) || this.isFullname()) {
				return name;
			}
			return parent + schema.getDelimiter() + name;
		}

		private boolean match(List<String> filters, String parent) {
			if (columns.size() == 0) {
				for (String p : filters) {
					if (StringUtils.match(p, getFullname(parent))) {
						return true;
					}
				}
				return false;
			} else {
				int countMatch = columns.size();
				for (Column c : columns) {
					if (c.match(filters, getFullname(parent))) {
						return true;
					} else {
						countMatch--;
					}
				}
				return countMatch != 0;
			}
		}

		public void merge(Column that) {
			this.setGroup(that.getGroup());
			if (that.columns == null || that.columns.size() == 0) return;
			Map<String, Column> thatMap = that.columns.stream()
					.collect(
							Collectors.toMap(
									Column::getName, Function.identity()
							)
					);
			Set<String> thisColumns = this.columns.stream().map(Column::getName).collect(Collectors.toSet());
			//get new childs from that (absent in this)
			List<Column> newColumns = that.columns.stream()
					.filter(c -> !thisColumns.contains(c.getName()))
					.peek(c -> c.setSchema(this.getSchema()))
					.collect(Collectors.toList());

			//merging childs
			this.columns.stream()
					.filter(c -> thatMap.containsKey(c.getName()))
					.forEach(
							column -> {
								column.merge(thatMap.get(column.getName()));
							}
					);
			//add new columns
			this.columns.addAll(newColumns);
		}
	}

	public enum GroupPolicy {
		CONCAT, ARRAY, NO_GROUP, COLUMNS
	}
}
