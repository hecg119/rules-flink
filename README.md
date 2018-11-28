# rules-flink

Build: 

- change mainClass in _pom.xml_ to HorizontalRulesJob or VerticalRulesJob

- then: `mvn clean package`

Run: 

`java -jar horizontal.jar data/ELEC.arff 8 100` 

or 

`java -jar vertical.jar data/ELEC.arff 8 100 5000`
