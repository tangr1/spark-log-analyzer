#!/bin/bash

class=$1
shift
spark-submit --class "com.ctofunds.dd.nginx.$class" --master "local[4]" target/scala-2.10/dd_2.10-0.0.16.jar $@
