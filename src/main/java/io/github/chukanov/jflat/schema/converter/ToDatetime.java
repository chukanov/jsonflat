package io.github.chukanov.jflat.schema.converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

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

@NoArgsConstructor
@Data
public class ToDatetime implements Converter {
	String pattern = "yyyy-MM-dd'T'HH:mm:ss";


	@Override
	public JsonNode convert(JsonNode value) {
		try {
			Instant instant = Instant.ofEpochMilli(Long.parseLong(value.asText()));
			return new TextNode(DateTimeFormatter.ofPattern(pattern).withZone(ZoneId.systemDefault()).format(instant));
		} catch (Exception e) {
			return value;
		}
	}
}
