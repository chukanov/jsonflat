package io.github.jsonflat;

import io.github.jsonflat.utils.StringUtils;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.*;
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
 * @author Evgeniy Chukanov
 */

public class StringUtilsTest {
  @Test
  public void testBlank(){
    assertTrue(StringUtils.isBlank(""));
    assertTrue(StringUtils.isBlank(null));
    assertTrue(StringUtils.isBlank(" "));
    assertTrue(StringUtils.isBlank("                                                                            "));
    assertTrue(StringUtils.isBlank("\t"));
    assertTrue(StringUtils.isBlank("\n"));
    assertTrue(StringUtils.isBlank("   \t  "));
    assertTrue(StringUtils.isBlank("    \n   "));
  }
  @Test
  public void testNotBlank(){
    assertTrue(StringUtils.isNotBlank("a"));
    assertTrue(StringUtils.isNotBlank("null"));
    assertTrue(StringUtils.isNotBlank(" a "));
  }

  @Test
  public void testMatch(){
    assertTrue(StringUtils.match("a", "a"));
    assertTrue(StringUtils.match("a*", "a"));
    assertTrue(StringUtils.match("a*b", "ab"));
    assertTrue(StringUtils.match("a**", "a"));
    assertTrue(StringUtils.match("a*", "abcdefghijk"));
    assertTrue(StringUtils.match("a**", "abcdefghijk"));
    assertTrue(StringUtils.match("a**?", "abcdefghijk"));
    assertTrue(StringUtils.match("a*k", "abcdefghijk"));
    assertTrue(StringUtils.match("*", UUID.randomUUID().toString()));
    assertTrue(StringUtils.match("a?c", "abc"));
    assertTrue(StringUtils.match("a?", "ab"));
    assertTrue(StringUtils.match("a??", "abc"));
    assertTrue(StringUtils.match("???", "abc"));
    assertTrue(StringUtils.match("?bc", "abc"));
    assertTrue(StringUtils.match("??c", "abc"));
    assertTrue(StringUtils.match("????????-????-????-????-????????????", UUID.randomUUID().toString()));
  }

  @Test
  public void testNotMatch(){
    assertFalse(StringUtils.match("a?", "a"));
    assertFalse(StringUtils.match("a?b", "ab"));
    assertFalse(StringUtils.match("a", "b"));
    assertFalse(StringUtils.match("a", "aa"));
    assertFalse(StringUtils.match("b*", "a"));
    assertFalse(StringUtils.match("a*z", "abcdefghijk"));
    assertFalse(StringUtils.match("a?c", "abcd"));
    assertFalse(StringUtils.match("a??", "a"));
    assertFalse(StringUtils.match("???", "ab"));
    assertFalse(StringUtils.match("????????.????.????.????.????????????", UUID.randomUUID().toString()));
  }

  @Test
  public void logPreprocessJson() {
    String log = "{\"a\":\"b\"}";
    assertEquals(log, StringUtils.preprocessLog(log));
  }

  @Test
  public void logPreprocessJsonArray() {
    String log = "[{\"a\":\"b\"}]";
    assertEquals(log, StringUtils.preprocessLog(log));
  }

  @Test
  public void logPreprocessJsonBad() {
    String log = "\"a\":\"b\"";
    assertEquals(log, StringUtils.preprocessLog(log));
  }

  @Test
  public void logPreprocessLog() {
    String log = "2019-08-09T03:41:48Z    app   {\"a\":\"b\"}";
    assertEquals("{\"a\":\"b\"}",StringUtils.preprocessLog(log));
  }

  @Test
  public void logPreprocessLogArray() {
    String log = "2019-08-09T03:41:48Z    app   [{\"a\":\"b\"}]";
    assertEquals("[{\"a\":\"b\"}]", StringUtils.preprocessLog(log));
  }

}
