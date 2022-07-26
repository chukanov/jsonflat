package io.github.chukanov.jflat.utils;

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

public final class StringUtils {
	public static boolean isBlank(String s) {
		return s == null || s.length() == 0 || s.trim().length() == 0;
	}

	public static boolean isNotBlank(String s) {
		return !isBlank(s);
	}

	/**
	 * The main function that checks if two given strings match. The first string may contain wildcard characters
	 * based on https://www.geeksforgeeks.org/wildcard-character-matching/
	 **/
	public static boolean match(String pattern, String text) {
		pattern = deDuplicateChars(pattern, '*');
		// If we reach at the end of both strings,
		// we are done
		if (pattern.length() == 0 && text.length() == 0)
			return true;

		// Make sure that the characters after '*'
		// are present in second string.
		// This function assumes that the first
		// string will not contain two consecutive '*'
		if (pattern.length() > 1 && pattern.charAt(0) == '*' &&
				text.length() == 0)
			return false;

		if (pattern.length() > 0 && pattern.charAt(0) == '?' && text.length() == 0) return false;

		// If the first string contains '?',
		// or current characters of both strings match
		if ((pattern.length() > 0 && pattern.charAt(0) == '?') ||
				(pattern.length() != 0 && text.length() != 0 &&
						pattern.charAt(0) == text.charAt(0)))
			return match(pattern.substring(1),
					text.substring(1));

		// If there is *, then there are two possibilities
		// a) We consider current character of second string
		// b) We ignore current character of second string.
		if (pattern.length() > 0 && pattern.charAt(0) == '*')
			return match(pattern.substring(1), text) ||
					match(pattern, text.substring(1));
		return false;
	}

	public static String deDuplicateChars(String s, char c) {
		if (StringUtils.isNotBlank(s)) {
			char[] chr = s.toCharArray();
			char r1, r2;
			StringBuilder res = new StringBuilder();
			if (chr.length >= 2) {
				for (int i = 0; i < chr.length - 1; i++) {
					r1 = chr[i];
					r2 = chr[i + 1];
					if (r1 == c && r2 == c) {
						continue;
					}
					res.append(r1);
				}
				res.append(chr[chr.length - 1]);
			} else {
				return s;
			}
			return res.toString();
		} else {
			return "";
		}
	}

	public static String preprocessLog(String line) {
		int index = line.indexOf('{');
		if (index == 0) return line;
		int index2 = line.indexOf('[');
		if (index2 == 0) return line;
		if (index2 > 0) {
			index = Math.min(index, index2);
		}
		if (index > 0) {
			return line.substring(index);
		}
		return line;
	}

	public static String escapeQuotes(String text) {
		if (StringUtils.isBlank(text)) return "";
		return text.replaceAll("\"", "\\\\\"");
	}
}
