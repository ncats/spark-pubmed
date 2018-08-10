#!/bin/sh
cmd="../spark-2.3.1-bin-hadoop2.7/bin/spark-submit --jars ../../../java/aws-java-sdk-1.11.381/lib/aws-java-sdk-1.11.381.jar,spark-xml_2.11-0.4.1.jar --class pubmed.SparkPubMed target/scala-2.11/sparkpubmed_2.11-0.1-SNAPSHOT.jar $*"
echo $cmd
exec $cmd
