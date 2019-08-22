/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
 * Decodes a property encoded by {@link EncodeProperty}.
 */
public class DecodeProperty<E extends Element> implements TransformationFunction<E> {

  /**
   * The property key to decode.
   */
  private final String key;

  /**
   * The base64 decoder.
   */
  private transient Base64.Decoder decoder = Base64.getDecoder();

  public DecodeProperty(String key) {
    this.key = key;
  }

  @Override
  public E apply(E current, E transformed) {
    if (current.hasProperty(key)) {
      current.setProperty(key,
        PropertyValue.fromRawBytes(decode(current.getPropertyValue(key))));
    }
    return current;
  }

  /**
   * Helper function initializing the decoder and decoding the property value.
   *
   * @param pv The property storing a base64 encoded string.
   * @return The decoded raw property.
   */
  private byte[] decode(PropertyValue pv) {
    if (decoder == null) decoder = Base64.getDecoder();
    return decoder.decode(pv.getString());
  }
}
