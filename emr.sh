#!/bin/sh
aws emr add-steps \
    --cluster-id j-W02FSE9TJ7B7 \
    --steps Type=Spark,Name=SparkPubMed,Args='[--deploy-mode,cluster,--master,yarn,--conf,spark.yarn.submit.waitAppCompletion=true,--class,pubmed.SparkPubMed,--jars,s3://pubmed.ncats.io/spark-xml_2.11-0.4.1.jar,s3://pubmed.ncats.io/sparkpubmed_2.11-0.1-SNAPSHOT.jar,pubmed.ncats.io,baseline_07312018]' \
    --region us-east-1 \
    --profile default
