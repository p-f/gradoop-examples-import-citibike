/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.examples.dataintegration.citibike.operators.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.api.entities.Attributed;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * A map function unquoting all String-type property values of an element.
 *
 * @param <E> The element type.
 */
public class UnquoteElementProperties<E extends Attributed> implements MapFunction<E, E> {

  @Override
  public E map(E element) {
    final Properties properties = element.getProperties();
    if (properties == null) {
      return element;
    }
    for (Property property : properties) {
      final PropertyValue value = property.getValue();
      if (!value.isString()) {
        continue;
      }
      String stringValue = value.getString();
      while (stringValue.length() >= 2 && stringValue.startsWith("\"") && stringValue.endsWith("\"")) {
        stringValue = stringValue.substring(1, stringValue.length() - 1);
      }
      value.setString(stringValue);
    }
    return element;
  }
}
