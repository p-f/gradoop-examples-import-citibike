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
package org.gradoop.examples.dataintegration.citibike;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.examples.dataintegration.citibike.metadata.MetaDataUtil;
import org.gradoop.examples.dataintegration.citibike.metadata.station.StationMetadata;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.temporal.io.impl.csv.TemporalCSVDataSink;
import org.gradoop.temporal.util.TemporalGradoopConfig;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * An importer for {@code https://www.citibikenyc.com/system-data} data.
 */
public class CitiBikeImporter implements ProgramDescription {

  /**
   * Main class for this Gradoop importer.
   *
   * @param args The command line arguments.
   */
  public static void main(String[] args) throws Exception {
    Options cliOption = new Options();
    cliOption.addOption(new Option("t", "temporal", false, "Create a temporal graph."));
    cliOption.addOption(new Option("i", "input", true, "Input path. (REQUIRED)"));
    cliOption.addOption(new Option("o", "output", true, "Output path. (REQUIRED)"));
    cliOption.addOption(new Option("h", "help", false, "Show this help."));
    cliOption.addOption(new Option("f", "force-overwrite", false, "Overwrite existing data."));
    cliOption.addOption(new Option("s", "schema", true,
      "Select the target schema 'TRIPS_AS_EDGES' or 'TRIPS_AS_VERTICES'."));
    cliOption.addOption(new Option("m", "metadata", true, "Attach station metadata."));
    CommandLine parsedOptions = new DefaultParser().parse(cliOption, args);
    if (parsedOptions.hasOption('h')) {
      new HelpFormatter().printHelp(CitiBikeImporter.class.getName(), cliOption, true);
      return;
    }
    if (!(parsedOptions.hasOption('i') && parsedOptions.hasOption('o'))) {
      System.err.println("No input- and output-path given.");
      System.err.println("See --help for more infos.");
      return;
    }
    final String inputPath = parsedOptions.getOptionValue('i');
    final String outputPath = parsedOptions.getOptionValue('o');
    final boolean temporal = parsedOptions.hasOption('t');
    final boolean overWrite = parsedOptions.hasOption('f');
    TargetGraphSchema schema;
    if (parsedOptions.hasOption('s')) {
      final String schemaString = parsedOptions.getOptionValue('s');
      try {
        schema = TargetGraphSchema.valueOf(schemaString);
      } catch (IllegalArgumentException iae) {
        System.err.println("Unsupported schema: " + schemaString);
        System.err.println("Has to be one of: " +
                Arrays.stream(TargetGraphSchema.values())
                        .map(Object::toString)
                        .collect(Collectors.joining(", ")));
        return;
      }
    } else {
      schema = TargetGraphSchema.TRIPS_AS_VERTICES;
    }

    StationMetadata metadata = null;
    if (parsedOptions.hasOption('m')) {
      metadata = MetaDataUtil.readStationData(parsedOptions.getOptionValue('m'));
    }
    ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(environment);
    TemporalGradoopConfig temporalGradoopConfig = TemporalGradoopConfig.fromGradoopFlinkConfig(config);
    CitibikeDataImporter source = new CitibikeDataImporter(inputPath, metadata, schema,
      temporalGradoopConfig);
    if (temporal) {
      source.getTemporalGraph().writeTo(new TemporalCSVDataSink(outputPath, config), overWrite);
    } else {
      source.getLogicalGraph().writeTo(new CSVDataSink(outputPath, config), overWrite);
    }
    environment.execute("CitiBike Data Importer from " + inputPath);
  }

  @Override
  public String getDescription() {
    return "Gradoop Data Importer for data provided by www.citibikenyc.com/system-data\n";
  }
}
