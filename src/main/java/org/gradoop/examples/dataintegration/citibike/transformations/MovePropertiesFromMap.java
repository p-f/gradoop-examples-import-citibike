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
package org.gradoop.examples.dataintegration.citibike.transformations;

import org.gradoop.common.model.api.entities.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.TransformationFunction;

import java.util.Map;

/**
 * A transformation function that moves entries of a map-type property to new properties.<p>
 * If the property is not set, it will be ignored, if the property is not a map, an exception will be
 * thrown. If keys of map entries are not strings, they will be converted using {@link Object#toString()}.
 */
public class MovePropertiesFromMap<E extends Element> implements TransformationFunction<E> {

  /**
   * The key of the map-typed property value.
   */
  private final String propertyKey;

  public MovePropertiesFromMap(String propertyKey) {
    this.propertyKey = propertyKey;
  }

  @Override
  public E apply(E current, E transformed) {
    if (!current.hasProperty(propertyKey)) {
      return current;
    }
    PropertyValue mapValue = current.getPropertyValue(propertyKey);
    if (!mapValue.isMap()) {
      throw new IllegalArgumentException("Property is not a map: " + mapValue);
    }
    for (Map.Entry<PropertyValue, PropertyValue> entry : mapValue.getMap().entrySet()) {
      final PropertyValue key = entry.getKey();
      current.setProperty(key.isString() ? key.getString() : key.getObject().toString(), entry.getValue());
    }
    current.removeProperty(propertyKey);
    return current;
  }
}
