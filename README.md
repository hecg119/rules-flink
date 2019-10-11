# Adaptive decision rules

Distributed decision rules for data streams, implemented using Apache Flink.

### Install:
* Scala 2.11.12
* Flink 1.6.1
* Maven
* Dependencies given in *pom.xml*

### Build: 

* Change `mainClass` in _pom.xml_ to `HorizontalRulesJob` or `VerticalRulesJob`

* Then: `mvn clean package`

### Run: 

`java -jar horizontal.jar data/ELEC.arff 8 100` 

or 

`java -jar vertical.jar data/ELEC.arff 8 100 5000`
