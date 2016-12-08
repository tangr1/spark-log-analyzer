#!/bin/bash

class=$1
shift
spark-submit --jars /Users/tangrui/workspace/spark-1.6.0-bin-hadoop2.6/libs/* --class "com.ctofunds.dd.$class" --master "local[4]" target/scala-2.10/dd_2.10-0.0.16.jar $@
