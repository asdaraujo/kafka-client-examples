#!/bin/bash
mvn exec:java -Dexec.mainClass=com.cloudera.examples.Consumer -Dexec.args="localhost:9092 test 100 10"
