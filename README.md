# Spark Cassandra Stress

A tool for testing the [DataStax Spark Cassandra Connector](https://github.com/datastax/spark-cassandra-connector) against both [Apache
Cassandra](https://cassandra.apache.org/) (TM) and [DataStax Enterprise](http://www.datastax.com/products/datastax-enterprise) (DSE) with either bundled libraries from
DSE, Maven, or the connector built from source!

## Building

This project is built using Gradle and can be built in three exciting ways:

1. Using the Connector and Spark libraries installed by DataStax Enterprise
2. Using libraries downloaded from maven
3. Using libraries assembled fresh from your local Spark Cassandra Connector Repository

The jar can be built using 

    ./gradlew jar -Pagainst=[type]
    
Where type is one of `dse`,`maven` or `source`

### DSE Options

DSE libraries are located by looking for the installation of DSE on your machine.
Change environment variables `DSE_HOME` and `DSE_RESOURCES` if your installation
differs from the default.

Defaults

    DSE_HOME=$HOME/dse
    DSE_RESOURCES=$HOME/dse/resources
    
### Maven Options

When getting libraries from Maven we need to specify the Connector version and
Spark Version libraries to compile against. Change environment variables 
`CONNECTOR_VERSION` and `SPARK_VERSION` to the artifacts you would like to 
use.

Defaults

    CONNECTOR_VERSION=1.2.0-rc2
    SPARK_VERSION=1.2.1
    
### Source Options

Gradle will attempt to clean and build the assembly jar for the Spark Connector
looking for the repository in environment variable `SPARKCC_HOME`. This will 
build whatever commit the connector is currently at.

Default

    SPARKCC_HOME=$HOME/repos/spark-cassandra-connector/


## Running

There are many options which can be used to configure your run of
Spark Cassandra Stress but the two main invocations are either using
`dse spark-submit` or `spark-submit`. 

See the `run.sh` script for examples or use it to launch the program.

    ./run.sh [dse|apache] --help
    
Will bring up the built in help.

## License

Copyright 2014-17, DataStax, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
