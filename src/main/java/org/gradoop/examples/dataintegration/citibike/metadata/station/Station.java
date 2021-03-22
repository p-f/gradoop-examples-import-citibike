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
package org.gradoop.examples.dataintegration.citibike.metadata.station;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.net.URL;
import java.util.EnumSet;

/**
 * Represents a station.
 */
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@ToString
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
public class Station implements Comparable<Station>, Serializable {

  /**
   * The station ID.
   */
  @EqualsAndHashCode.Include
  private int stationId;

  /**
   * The external ID.
   */
  private String externalId;

  /**
   * The station name.
   */
  private String name;

  /**
   * The short station name.
   */
  private String shortName;

  /**
   * The station latitude.
   */
  private double lat;

  /**
   * The station longitude.
   */
  private double lon;

  /**
   * The region ID.
   */
  private short regionId;

  /**
   * A set of rental methods.
   */
  private EnumSet<RentalMethod> rentalMethods;

  /**
   * The stations capacity.
   */
  private int capacity;

  /**
   * The rental URL.
   */
  private URL rentalUrl;

  /**
   * Value of the {@code has_kiosk} field.
   */
  private boolean hasKiosk;

  @Override
  public int compareTo(Station other) {
    return this.stationId - other.stationId;
  }
}
