package io.github.chukanov.jflat.model;

import lombok.AllArgsConstructor;
import lombok.Value;

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

@Value
@AllArgsConstructor
public class CompositeCell implements Cell, Iterable<RowValue> {
	List<RowValue> values;

	public Iterator<RowValue> iterator() {
		return new ColumnsIterator(values);

	}

	@Override
	public boolean isEmpty() {
		for (RowValue nv : this) {
			if (!(nv.getCell().isEmpty())) return false;
		}
		return true;
	}

	@Override
	public boolean isRequired() {
		for (RowValue nv : this) {
			if (nv.getCell().isRequired()) return true;
		}
		return false;
	}

	private static class ColumnsIterator implements Iterator<RowValue> {
		LinkedList<Iterator<RowValue>> stack;

		public ColumnsIterator(Iterable<RowValue> iterable) {
			this.stack = new LinkedList<>();
			stack.push(iterable.iterator());
		}

		@Override
		public boolean hasNext() {
			Iterator<RowValue> currentIterator = stack.peek();
			if (currentIterator == null) return false;
			if (currentIterator.hasNext()) {
				return true;
			} else {
				stack.pop();
				return hasNext();
			}
		}

		@Override
		public RowValue next() {
			Iterator<RowValue> currentIterator = stack.peek();
			if (currentIterator == null) return null;
			RowValue value = currentIterator.next();
			if (value.getCell() instanceof CompositeCell) {
				stack.push(((CompositeCell) value.getCell()).getValues().iterator());
				return next();
			} else {
				return value;
			}
		}
	}

}
