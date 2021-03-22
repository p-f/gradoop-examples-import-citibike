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
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.TransformationFunction;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * A transformation function that moves certain properties of an element to a map and stores that map as
 * a property on the element. The property keys will be transformed before storing them in the map. The
 * original properties will be removed.
 */
public class RenameAndMovePropertiesToMap<E extends Element> implements TransformationFunction<E> {

  /**
   * A list of property keys to move to the map.
   */
  private final List<String> propertyKeys;

  /**
   * The property key used to store the new property value.
   */
  private final String targetKey;

  /**
   * The property key transformation function applied before the property is stored in the map.
   */
  private final Function<String, String> keyTransformer;

  public RenameAndMovePropertiesToMap(List<String> propertyKeys, String targetKey,
    Function<String, String> keyTransformer) {
    this.propertyKeys = propertyKeys;
    this.targetKey = targetKey;
    propertyKeys.sort(Comparator.naturalOrder());
    this.keyTransformer = keyTransformer;
  }

  @Override
  public E apply(E current, E transformed) {
    if (current.getProperties() == null) {
      return current;
    }
    Map<PropertyValue, PropertyValue> mapProperty = new HashMap<>();
    for (String k : propertyKeys) {
      if (current.hasProperty(k)) {
        mapProperty.put(PropertyValue.create(keyTransformer.apply(k)), current.getPropertyValue(k));
        current.removeProperty(k);
      }
    }
    current.setProperty(targetKey, PropertyValue.create(mapProperty));
    return current;
  }
}
