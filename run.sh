#!/bin/bash
mvn exec:java -Dexec.mainClass="dev.hadoop.ReportDriver" -Dexec.args="1 input/ output" 

