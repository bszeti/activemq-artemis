export "MAVEN_OPTS=-Dorg.slf4j.simpleLogger.log.org.apache.activemq.artemis.jms.example=debug -Dorg.slf4j.simpleLogger.log.org.apache.qpid.jms=trace"
export "MAVEN_OPTS=$MAVEN_OPTS -Dorg.slf4j.simpleLogger.showDateTime=true -Dorg.slf4j.simpleLogger.showThreadName=true"
export "MAVEN_OPTS=$MAVEN_OPTS -Dorg.slf4j.simpleLogger.logFile=out.log"
mvn clean install -Dactivemq.basedir=${ARTEMIS_HOME}
