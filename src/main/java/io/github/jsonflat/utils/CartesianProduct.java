package io.github.jsonflat.utils;

import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.toList;

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

public class CartesianProduct {

	public static <T> List<List<T>> cartesianProduct(List<? extends List<? extends T>> lists) {
		lists = lists.stream().filter(list -> !list.isEmpty()).collect(toList());
		List<List<T>> product = new ArrayList<>(lists.size() * 2);
		for (List<? extends T> list : lists) {
			List<List<T>> newProduct = new ArrayList<>(list.size() * 2);
			for (T listElement : list) {
				if (product.isEmpty()) {
					List<T> newProductList = new ArrayList<>();
					newProductList.add(listElement);
					newProduct.add(newProductList);
				} else {
					for (List<T> productList : product) {
						List<T> newProductList = new ArrayList<>(productList);
						newProductList.add(listElement);
						newProduct.add(newProductList);
					}
				}
			}
			product = newProduct;
		}
		return product;
	}
}
 