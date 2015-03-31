h1. Spark Cassandra Stress

A tool for testing the Datastax Spark Connector against both Apache
Cassandra and DSE packaged Cassandra with either Apache Spark or DSE 
packaged Spark

h2. Building

This project is built using gradle and can be built in three(3) exciting ways.

1. Using the Connector and Spark libraries installed by Datastax Enterprise
2. Using libraries downloaded from maven
3. Using libraries assembled fresh from your local Spark Cassandra Connector Repository

The jar can be built using 

    ./gradlew jar -Pagainst=[type]
    
Where type is one of `dse`,`maven` or `source`

h3. DSE Options

DSE libraries are located by looking for the installation of DSE on your machine.
Change environment variables `DSE_HOME` and `DSE_RESOURCES` if your installation
differs from the default.


Defaults

    DSE_HOME=$HOME/dse
    DSE_RESOURCES=$HOME/dse/resources
    
h3. Maven Options

When getting libraries from maven we need to specify the Connector version and
Spark Version libraries to compile against. Change environment variables 
`CONNECTOR_VERSION` and `SPARK_VERSION` to the artifacts you would like to 
use.

Defaults

    CONNECTOR_VERSION=1.2.0-rc2
    SPARK_VERSION=1.2.1
    
h3. Source Options

Gradle will attempt to clean and build the assembly jar for the Spark Connector
looking for the repository in environment variable `SPARKCC_HOME`. This will 
build in whatever 

Default

    SPARKCC_HOME=$HOME/repos/spark-cassandra-connector/


h2. Running

There are many options which can be used to configure your run of
Spark Cassandra Stress but the two main invocations are either using
`dse spark-submit` or `spark-submit`. 

See the `run.sh` script for examples or use it to launch the program.

    ./run.sh [dse|apache] --help
    
Will bring up the built in help
