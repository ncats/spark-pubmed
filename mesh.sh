#!/bin/sh
aws emr add-steps \
    --cluster-id j-1S404WXUX5VTE \
    --steps Type=Spark,Name=SparkMesh,Args='[--deploy-mode,cluster,--master,yarn,--conf,spark.yarn.submit.waitAppCompletion=true,--class,pubmed.SparkMesh,s3://pubmed.ncats.io/jars/spark-pubmed_2.11-0.0.3.jar,pubmed.ncats.io,baseline_07312018.msh2]' \
    --region us-east-2 \
    --profile default
