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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.junit.Test;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.EnumSet;

import static org.junit.Assert.*;

/**
 * Test for JSON-serialization of {@link Station} objects.
 */
public class StationJsonSerializationTest extends GradoopFlinkTestBase {

  /**
   * An example station JSON object. (This is no real data.)
   */
  private final String stationJson = "{\n  \"station_id\": \"1337\",\n  \"external_id\": \"test_id\",\n" +
    "  \"name\": \"Test Station\",\n \"short_name\": \"420.69\",\n \"lat\": 51.339379,\n" +
    "  \"lon\": 12.379049,\n  \"region_id\": 69,\n  \"rental_methods\": [\"KEY\"],\n" +
    "  \"capacity\": 1337,\n  \"rental_url\": \"http://example.com/some_rental_url\",\n" +
    "  \"electric_bike_surcharge_waiver\": false,\n  \"eightd_has_key_dispenser\": false,\n" +
    "  \"has_kiosk\": true\n}";

  /**
   * An example station metadata JSON object. (This is no real data.)
   */
  private final String stationMetaData = "{\"last_updated\": 13371337, \"ttl\": 10, \"data\": { " +
    "\"stations\": [" +
    stationJson + "," + stationJson
    + "]}}";

  /**
   * Test JSON-deserialization of a {@link Station}.
   *
   * @throws IOException When (de)serialization fails.
   */
  @Test
  public void testStationDeserializaion() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    final Object result = mapper.readerFor(Station.class).readValue(stationJson);
    assertTrue(result instanceof Station);
    checkStationData((Station) result);
  }

  /**
   * Test deserialization of a full {@link StationMetadata} object.
   *
   * @throws IOException When serialization of the object fails.
   */
  @Test
  public void testStationMetadataSerialization() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    final ObjectReader metaDataReader = mapper.readerFor(StationMetadata.class);
    final Object deserialized = metaDataReader.readValue(stationMetaData);
    assertTrue(deserialized instanceof StationMetadata);
    final StationMetadata md = (StationMetadata) deserialized;
    assertEquals(13371337L, md.getLastUpdated());
    assertEquals(10, md.getTtl());
    final Station[] stations = md.getData().getStations();
    assertEquals(2, stations.length);
    checkStationData(stations[0]);
    checkStationData(stations[1]);
    assertNotSame(stations[0], stations[1]);
    final ObjectWriter metaDataWriter = mapper.writerFor(StationMetadata.class);
    // Ignore value, just test if serialization works.
    metaDataWriter.writeValueAsString(md);
  }

  /**
   * Check if the data of a station is the same as the JSON data provided by this class.
   *
   * @param station The station to check.
   */
  private void checkStationData(Station station) throws MalformedURLException {
    assertEquals(1337, station.getStationId());
    assertEquals("test_id", station.getExternalId());
    assertEquals("Test Station", station.getName());
    assertEquals("420.69", station.getShortName());
    assertEquals(51.339379d, station.getLat(), 0.0000001d);
    assertEquals(12.379049d, station.getLon(), 0.0000001d);
    assertEquals(69, station.getRegionId());
    assertEquals(EnumSet.of(RentalMethod.KEY), station.getRentalMethods());
    assertEquals(69, station.getRegionId());
    assertEquals(new URL("http://example.com/some_rental_url"), station.getRentalUrl());
    assertTrue(station.isHasKiosk());
  }
}