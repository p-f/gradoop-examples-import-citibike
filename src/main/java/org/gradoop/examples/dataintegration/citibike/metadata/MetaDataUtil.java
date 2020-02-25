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
package org.gradoop.examples.dataintegration.citibike.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.gradoop.examples.dataintegration.citibike.metadata.station.Station;
import org.gradoop.examples.dataintegration.citibike.metadata.station.StationMetadata;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Objects;

/**
 * A utility class used to work with metadata objects.
 */
public final class MetaDataUtil {

  /**
   * Get an array of stations ordered by their id from a metadata object.
   *
   * @param metadata The station metadata.
   * @return The stations, ordered by ID.
   */
  public static Station[] getStationsById(StationMetadata metadata) {
    Station[] stations = Objects.requireNonNull(metadata).getData().getStations();
    Arrays.sort(stations);
    return stations;
  }

  /**
   * Read a metadata object from a path.
   *
   * @param path The metadata path.
   * @return The read and parsed metadata object.
   * @throws IOException when reading the file fails.
   */
  public static StationMetadata readStationData(String path) throws IOException {
    return readStationData(path, FileSystem.get(FileSystem.getDefaultFsUri()));
  }

  /**
   * Read the metadata object from a path.
   *
   * @param path The metadata path.
   * @param fs The filesystem to read from.
   * @return The read and parsed metadata object.
   * @throws IOException when reading the file fails.
   */
  public static StationMetadata readStationData(String path, FileSystem fs) throws IOException {
    InputStream in = Objects.requireNonNull(fs).open(new Path(path));
    StationMetadata md = new ObjectMapper().readerFor(StationMetadata.class).readValue(in);
    in.close();
    return md;
  }
}
