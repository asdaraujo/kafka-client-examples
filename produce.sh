#!/bin/bash
mvn exec:java -Dexec.mainClass=com.cloudera.examples.Producer -Dexec.args="localhost:9092 test 1000 1 100 1 1500 4500"
