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
package org.gradoop.examples.dataintegration.citibike.transformations.workarounds;

import org.gradoop.common.model.api.entities.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.TransformationFunction;

import java.util.Base64;

/**
 * Encodes a certain property to a string. This will create a {@link String} from the {@code byte}
 * -representation of a {@link org.gradoop.common.model.impl.properties.PropertyValue} using Base64 encoding.
 */
public class EncodeProperty<E extends Element> implements TransformationFunction<E> {

  /**
   * The property key to transform.
   */
  private final String key;

  /**
   * The base64 encoder.
   */
  private transient Base64.Encoder encoder = Base64.getEncoder();

  public EncodeProperty(String key) {
    this.key = key;
  }

  @Override
  public E apply(E current, E transformed) {
    if (current.hasProperty(key)) {
      String strValue = encode(current.getPropertyValue(key));
      current.setProperty(key, PropertyValue.create(strValue));
    }
    return current;
  }

  /**
   * Helper function initializing the encoder and encoding the property value.
   *
   * @param pv The property value.
   * @return The base64 encoded property.
   */
  private String encode(PropertyValue pv) {
    if (encoder == null) encoder = Base64.getEncoder();
    return encoder.encodeToString(pv.getRawBytes());
  }
}
