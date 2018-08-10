Simple Spark setup to process PubMed
====================================

This repository provides a simple setup to process PubMed with
Spark on a local cluster or via AWS' Elastic MapReduce (EMR) service.

```
sbt package
```

For local Spark cluster, use the script ```submit.sh``` as an example
on how to do Spark submission. Similarly, the script ```emr.sh```
provides an example for EMR setup.
