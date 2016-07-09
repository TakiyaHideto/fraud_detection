#!/usr/bin/env bash
mvn install:install-file -DgroupId=mysql -DartifactId=connector -Dversion=5.1.21 -Dpackaging=jar -Dfile=lib/mysql-connector-java-5.1.21-bin.jar
mvn install:install-file -DgroupId=com.google.protobuf -DartifactId=protobuf-java -Dversion=2.5.0 -Dpackaging=jar -Dfile=lib/protobuf-java-2.5.0.jar
