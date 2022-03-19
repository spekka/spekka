# Spekka Stateful

[![javadoc](https://javadoc.io/badge2/io.github.spekka/spekka-stateful_2.13/javadoc.svg)](https://javadoc.io/doc/io.github.spekka/spekka-stateful_2.13/)
[![maven](https://img.shields.io/maven-central/v/io.github.spekka/spekka-stateful_2.13)](https://mvnrepository.com/artifact/io.github.spekka/spekka-stateful_2.13/)

To use this library add the following dependencies:

@@dependency[sbt,Maven,Gradle] {
  symbol="AkkaVersion"  value="2.6.16"
  group="io.github.spekka"  artifact="spekka-stateful_$scala.binary.version$"  version="$project.version$"
  group2="com.typesafe.akka" artifact2="akka-stream_$scala.binary.version$"  version="$project.version$" version2="AkkaVersion"
  group3="com.typesafe.akka" artifact3="akka-stream-typed_$scala.binary.version$"  version="$project.version$" version3="AkkaVersion"
  group4="com.typesafe.akka" artifact4="akka-actor-typed_$scala.binary.version$"  version="$project.version$" version4="AkkaVersion"
}
