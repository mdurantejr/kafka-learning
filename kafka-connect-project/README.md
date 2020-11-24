Welcome to your new Kafka Connect connector!

# Running in development

```
mvn clean package
export CLASSPATH="$(find target/ -type f -name '*.jar'| grep '\-package' | tr '\n' ':')"
$CONFLUENT_HOME/bin/connect-standalone $CONFLUENT_HOME/etc/schema-registry/connect-avro-standalone.properties config/MySourceConnector.properties
```

For real example, see:  
https://github.com/simplesteph/kafka-connect-github-source  

For the maven connect archetype:  
https://github.com/jcustenborder/kafka-connect-archtype