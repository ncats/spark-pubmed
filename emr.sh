#!/bin/sh
aws emr add-steps \
    --cluster-id j-1S404WXUX5VTE \
    --steps Type=Spark,Name=SparkPubMed,Args='[--deploy-mode,cluster,--master,yarn,--conf,spark.yarn.submit.waitAppCompletion=true,--class,pubmed.SparkPubMed,--jars,s3://pubmed.ncats.io/jars/spark-xml_2.11-0.4.1.jar,s3://pubmed.ncats.io/jars/spark-pubmed_2.11-0.0.3.jar,pubmed.ncats.io,baseline_07312018,baseline_07312018.msh2]' \
    --region us-east-2 \
    --profile default
