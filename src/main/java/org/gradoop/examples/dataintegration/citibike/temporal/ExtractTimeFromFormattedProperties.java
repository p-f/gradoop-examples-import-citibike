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
package org.gradoop.examples.dataintegration.citibike.temporal;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.temporal.model.api.functions.TimeIntervalExtractor;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Extract valid times from property values set on elements.<p>
 * This will expect properties to be Strings storing a formatted date.<p>
 * This will try multiple {@link DateTimeFormatter time formats} and use the first valid result.
 *
 * @param <E> The type of the elements.
 */
public class ExtractTimeFromFormattedProperties<E extends Element> implements TimeIntervalExtractor<E> {

  /**
   * Logger for this class.
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(ExtractTimeFromFormattedProperties.class);

  /**
   * The property key storing the start time.
   */
  private final String keyStartTime;

  /**
   * The property key storing the end time.
   */
  private final String keyEndTime;

  /**
   * Date formats to try for parsing.
   */
  private final String[] formatStrings;

  /**
   * Formatters used to parse the date.
   * This function will try to parse a date string will all formats in the list, in order. The first
   * successful try will be used.
   */
  private transient List<DateTimeFormatter> formats;

  /**
   * Reduce object instantiations.
   */
  private final Tuple2<Long, Long> reuse = new Tuple2<>();

  /**
   * Initialize this extractor.
   *
   * @param keyStartTime The property key used to read the start time. (may be {@code null})
   * @param keyEndTime   The property key used to read the end time. (may be {@code null})
   * @param format       The formats of the time used to parse the property. (At least one is required.)
   */
  public ExtractTimeFromFormattedProperties(String keyStartTime, String keyEndTime, String... format) {
    this.keyStartTime = keyStartTime;
    this.keyEndTime = keyEndTime;
    if (format.length == 0) {
      throw new IllegalArgumentException("At least one format is expected.");
    }
    this.formatStrings = format;
    initParsers();
  }

  @Override
  public Tuple2<Long, Long> map(E value) {
    if (keyStartTime != null && value.hasProperty(keyStartTime)) {
      reuse.f0 = parse(value.getPropertyValue(keyStartTime));
    } else {
      reuse.f0 = TemporalElement.DEFAULT_TIME_FROM;
    }
    if (keyEndTime != null && value.hasProperty(keyEndTime)) {
      reuse.f1 = parse(value.getPropertyValue(keyEndTime));
    } else {
      reuse.f1 = TemporalElement.DEFAULT_TIME_TO;
    }
    if (reuse.f0 > reuse.f1) {
      LOGGER.warn("Valid time constraint (from > to) violated: {}, ({}-{})", value, reuse.f0, reuse.f1);
    }
    return reuse;
  }

  /**
   * Parse a property value to a temporal attribute.
   *
   * @param pv The property.
   * @return The time, in milliseconds since unix epoch.
   */
  private long parse(PropertyValue pv) {
    if (!pv.isString()) {
      throw new IllegalArgumentException("Property was expected to be a String, was " +
        pv.getType().getSimpleName());
    }
    final String dateString = pv.getString();
    IllegalArgumentException ex = null;
    if (formats == null) {
      initParsers();
    }
    for (DateTimeFormatter format : formats) {
      try {
        return format.parse(dateString, LocalDateTime::from).toInstant(ZoneOffset.UTC).toEpochMilli();
      } catch (DateTimeParseException pe) {
        if (ex != null) {
          ex.addSuppressed(pe);
        } else {
          ex = new IllegalArgumentException(pe);
        }
      }
    }
    throw ex != null ? ex : new IllegalStateException();
  }

  /**
   * Initialize the parsers. This is necessary since {@link #formats} will not be serialized.
   */
  private void initParsers() {
    this.formats = Arrays.stream(this.formatStrings).map(DateTimeFormatter::ofPattern)
      .collect(Collectors.toList());
  }
}
