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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.temporal.io.api.TemporalDataSource;
import org.gradoop.temporal.io.impl.csv.TemporalCSVDataSink;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.operators.aggregation.functions.AverageDuration;
import org.gradoop.temporal.util.TemporalGradoopConfig;

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
    Options cliOption = new Options();
    cliOption.addOption(new Option("t", "temporal", false,
      "Create a temporal graph."));
    cliOption.addOption(new Option("i", "input", true,
      "Input path. (REQUIRED)"));
    cliOption.addOption(new Option("o", "output", true,
      "Output path. (REQUIRED)"));
    cliOption.addOption(new Option("h", "help", false, "Show this help."));
    CommandLine parsedOptions = new DefaultParser().parse(cliOption, args);
    if (parsedOptions.hasOption('h')) {
      new HelpFormatter().printHelp(CitiBikeImporter.class.getName(), cliOption, true);
      return;
    }
    if (!(parsedOptions.hasOption('i') && parsedOptions.hasOption('o'))) {
      System.err.println("No input- and output-path given.");
      System.err.println("See --help for more infos.");
    }
    final String inputPath = parsedOptions.getOptionValue('i');
    final String outputPath = parsedOptions.getOptionValue('o');

    ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(environment);
    TemporalGradoopConfig temporalGradoopConfig = TemporalGradoopConfig.fromGradoopFlinkConfig(config);
    TemporalDataSource importer = new TemporalCitibikeDataImporter(inputPath, temporalGradoopConfig);
    TemporalGraph graph = importer.getTemporalGraph()
      .aggregate(new AverageDuration("avgTripDur", TimeDimension.VALID_TIME));
    graph.writeTo(new TemporalCSVDataSink(args[1], temporalGradoopConfig), true);
    environment.execute();
  }

  @Override
  public String getDescription() {
    return "Gradoop Data Importer for data provided by www.citibikenyc.com/system-data\n";
  }
}
