
rm -rf core/build/distributions/kafka_2.12-1.0.2-flipp.tgz
gradle releaseTarGz -PscalaVersion=2.12
rm -rf kafka_2.12-1.0.2-flipp/
tar -xvzf core/build/distributions/kafka_2.12-1.0.2-flipp.tgz

rm -rf ~/.m2/repository/org/apache/kafka/kafka-streams/1.0.2-flipp/kafka-streams-1.0.2-flipp.jar

mkdir -p ~/.m2/repository/org/apache/kafka/kafka-streams/1.0.2-flipp/
cp kafka_2.12-1.0.2-flipp/libs/kafka-streams-1.0.2-flipp.jar ~/.m2/repository/org/apache/kafka/kafka-streams/1.0.2-flipp/kafka-streams-1.0.2-flipp.jar
