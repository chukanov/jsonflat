package io.github.jsonflat;

import io.github.jsonflat.schema.AutoSchemaFactory;
import io.github.jsonflat.schema.JsonSchemaFactory;
import io.github.jsonflat.schema.Schema;
import io.github.jsonflat.schema.converter.Converter;
import io.github.jsonflat.schema.converter.ToString;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ReadmeMdTest {
	String jsonText = "{\n" +
			"  \"store\": {\n" +
			"    \"book\": [\n" +
			"      {\n" +
			"        \"category\": \"reference\",\n" +
			"        \"author\": \"Nigel Rees\",\n" +
			"        \"title\": \"Sayings of the Century\",\n" +
			"        \"price\": 8.95\n" +
			"      },\n" +
			"      {\n" +
			"        \"category\": \"fiction\",\n" +
			"        \"author\": \"Evelyn Waugh\",\n" +
			"        \"title\": \"Sword of Honour\",\n" +
			"        \"price\": 12.99\n" +
			"      },\n" +
			"      {\n" +
			"        \"category\": \"fiction\",\n" +
			"        \"author\": \"Herman Melville\",\n" +
			"        \"title\": \"Moby Dick\",\n" +
			"        \"isbn\": \"0-553-21311-3\",\n" +
			"        \"price\": 8.99\n" +
			"      },\n" +
			"      {\n" +
			"        \"category\": \"fiction\",\n" +
			"        \"author\": \"J. R. R. Tolkien\",\n" +
			"        \"title\": \"The Lord of the Rings\",\n" +
			"        \"isbn\": \"0-395-19395-8\",\n" +
			"        \"price\": 22.99\n" +
			"      }\n" +
			"    ],\n" +
			"    \"bicycle\": {\n" +
			"      \"color\": \"red\",\n" +
			"      \"price\": 19.95\n" +
			"    }\n" +
			"  },\n" +
			"  \"expensive\": 10\n" +
			"}";

	@Test
	public void testDoc1() throws IOException {

		Schema schema = AutoSchemaFactory.builder()
				.columnStringFilters(Arrays.asList("store_book_title", "store_book_price", "store_bicycle*"))
				.complexArraysGroup(Schema.GroupPolicy.COLUMNS)
				.build()
				.generate(jsonText);
		Transformer transformer = new Transformer(schema);
		List<String> result = transformer.transform(jsonText);
		//System.out.println(result);
	}

	@Test
	public void testDoc2() throws IOException {
		String schemaText = "{\n" +
				"  \"name\": \"Example schema\",\n" +
				"  \"version\": \"1.0\",\n" +
				"  \"columns\": [\n" +
				"    {\n" +
				"      \"name\": \"store\",\n" +
				"      \"columns\": [\n" +
				"        {\n" +
				"          \"name\": \"book\",\n" +
				"          \"path\": \"book[*]\",\n" +
				"          \"columns\": [\n" +
				"            {\"name\": \"type\", \"path\": \"category\"},\n" +
				"            {\"name\": \"author\"},\n" +
				"            {\"name\": \"title\"},\n" +
				"            {\"name\": \"isbn\", \"skipRowIfEmpty\": true},\n" +
				"            {\n" +
				"              \"name\": \"price\",\n" +
				"              \"fullname\": true,\n" +
				"              \"converter\": {\n" +
				"                \"class\": \"io.github.jsonflat.schema.converter.ToString\"\n" +
				"              }\n" +
				"            }\n" +
				"          ]\n" +
				"        }\n" +
				"      ]\n" +
				"    },\n" +
				"    {\n" +
				"      \"name\": \"expensive\"\n" +
				"    }\n" +
				"  ]\n" +
				"}";
		Schema schema = JsonSchemaFactory.builder().build().generate(schemaText);
		Transformer transformer = new Transformer(schema);
		List<String> result = transformer.transform(jsonText);
		//System.out.println(result);
	}

	@Test
	public void testDoc3() throws IOException {
		Schema schema = new Schema();
		schema.setColumns(
				Arrays.asList(
						new Schema.Column("store", "store", Arrays.asList(
								new Schema.Column("book", "book[*]",
										Arrays.asList(
												new Schema.Column("type", "category", Collections.emptyList(), schema),
												new Schema.Column("author", "author", Collections.emptyList(), schema),
												new Schema.Column("title", "title", Collections.emptyList(), schema),
												new Schema.Column("isbn", "isbn", Collections.emptyList(), Schema.GroupPolicy.NO_GROUP, false, false, true, Converter.DEFAULT, schema),
												new Schema.Column("price", "price", Collections.emptyList(), Schema.GroupPolicy.NO_GROUP, true, false, false, new ToString(), schema)
										),
										Schema.GroupPolicy.NO_GROUP, schema
								),
								new Schema.Column("expensive", "expensive", Collections.emptyList(), schema)
						), schema)
				)
		);
		Transformer transformer = new Transformer(schema);
		List<String> result = transformer.transform(jsonText);
		//System.out.println(result);
	}

}
