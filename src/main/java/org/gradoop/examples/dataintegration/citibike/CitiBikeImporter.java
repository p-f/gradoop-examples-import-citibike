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
package org.gradoop.examples.dataintegration.citibike;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * An importer for {@code https://www.citibikenyc.com/system-data} data.
 */
public class CitiBikeImporter implements ProgramDescription {

  /**
   * Main class for this example.
   *
   * @param args The command line arguments.
   */
  public static void main(String[] args) throws Exception {
    ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(environment);
    final String inputFile = args[0];
    DataSource importer = new CitibikeDataImporter(inputFile, config);
    importer.getLogicalGraph().print();
  }

  @Override
  public String getDescription() {
    return "Gradoop Data Importer for data provided by www.citibikenyc.com/system-data\n";
  }
}
