#!/bin/bash
mvn exec:java -Dexec.mainClass="dev.hadoop.v2.ReportDriver" -Dexec.args="1 input/ output" 

