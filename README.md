# Gradoop Example: Citi Bike NYC data importer

This is an example application for the distributed graph analytics framework
[Gradoop](https://github.com/dbs-leipzig/gradoop). This application is therefore
also built on [Apache Flink](https://flink.apache.org/).

This will read CSV data provided by
[Citi Bike NYC (Lyft, Inc.)](https://www.citibikenyc.com/system-data) and
convert it to either a logical graph or a temporal graph. The resulting graph
may be created with two possible schemas, where _trips_ are either mapped
to vertices or edges. When the graph is created as a temporal graph,
_trip_ elements will have their corresponding start and end times set as the
valid time. It is also possible to have [Station Metadata (also provided by Citi Bike NYC)](https://gbfs.citibikenyc.com/gbfs/en/station_information.json)
parsed and attached to station elements.

## Building

This application can be built using Maven. Simply run
```
mvn package
```
in the project directory. This application uses the latest snapshot version
`0.6.0-SNAPSHOT` of Gradoop and version `1.9.3` of Apache Flink.

If you want to build this application for other versions of Gradoop, you can use
```
mvn -Ddep.gradoop.version=YOUR_GRADOOP_VERSION package
```
Likewise, you can use `-Ddep.flink.version` to change to desired Flink version.
Note that only the mentioned versions are tested for this example.


## Execution

The build process with create a `JAR` package named
`example-import-citibike-VERSION-minimal.jar` in the `target` directory.
This application can be executed using a Flink client. Run
```
flink run THE_PACKAGE
```
where `THE_PACKAGE` is the path to the built project package.
You can also your IDE and start the application from the
`org.gradoop.examples.dataintegration.citibike.CitiBikeImporter` class.

### Configuration

The application takes a number of command line parameters:

|Parameter| Argument | Description |
|:-------:|----------|-------------|
| `-i`    | INPUT    | The input path, e.g., `/path/to/datasets/`. |
| `-o`    | OUTPUT   | The output path, e.g., `/path/to/output/`. |
| `-m`    | PATH     | The path of the metadata JSON file (optional), e.g., `/path/to/metadata.json` |
| `-s`    | SCHEMA   | The schema to create the graph in: `TRIPS_AS_EDGES` or `TRIPS_AS_VERTICES`. |
| `-t`    | (none)   | Creates a `TemporalGraph` instead of a `LogicalGraph`. |
| `-f`    | (none)   | Overwrite existing data. |
| `-h`    | (none)   | Show help. |

The input (`-i`) and output (`-o`) path options are required.
The input path is either a CSV file, or a directory containing multiple CSV files.
The output path is a directory.

Both paths may be HDFS paths. The output will be in the Gradoop CSV format,
either as a logical graph or as a temporal graph, with the `-t` flag.

# License

> This project is part of Gradoop.
>
>
> Copyright © 2014 - 2021 Leipzig University (Database Research Group)
>
> Licensed under the Apache License, Version 2.0 (the "License")
> you may not use this file except in compliance with the License.
> You may obtain a copy of the License at
>
>    http://www.apache.org/licenses/LICENSE-2.0
>
> Unless required by applicable law or agreed to in writing, software
> distributed under the License is distributed on an "AS IS" BASIS,
> WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
> See the License for the specific language governing permissions and
> limitations under the License.
