package io.github.jsonflat.model;

import lombok.AllArgsConstructor;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

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

@lombok.Value
@AllArgsConstructor
public class CompositeValue implements Value, Iterable<Cell> {
	List<Cell> values;

	public Iterator<Cell> iterator() {
		return new ColumnsIterator(values);
	}

	@Override
	public boolean isEmpty() {
		for (Cell nv : this) {
			if (!(nv.getValue().isEmpty())) return false;
		}
		return true;
	}

	@Override
	public boolean isRequired() {
		for (Cell nv : this) {
			if (nv.getValue().isRequired()) return true;
		}
		return false;
	}

	private static class ColumnsIterator implements Iterator<Cell> {
		LinkedList<Iterator<Cell>> stack;

		public ColumnsIterator(Iterable<Cell> iterable) {
			this.stack = new LinkedList<>();
			stack.push(iterable.iterator());
		}

		@Override
		public boolean hasNext() {
			Iterator<Cell> currentIterator = stack.peek();
			if (currentIterator == null) return false;
			if (currentIterator.hasNext()) {
				return true;
			} else {
				stack.pop();
				return hasNext();
			}
		}

		@Override
		public Cell next() {
			Iterator<Cell> currentIterator = stack.peek();
			if (currentIterator == null) return null;
			Cell value = currentIterator.next();
			if (value.getValue() instanceof CompositeValue) {
				stack.push(((CompositeValue) value.getValue()).getValues().iterator());
				return next();
			} else {
				return value;
			}
		}
	}

}
