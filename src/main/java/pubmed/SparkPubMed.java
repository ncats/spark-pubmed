package pubmed;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.zip.GZIPInputStream;
import java.util.function.Consumer;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.spark.*;
import org.apache.spark.sql.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class SparkPubMed {
    static void processEMR (SparkSession spark, String bucket,
                            String output, String input) throws Exception {
        Dataset<Row> df = spark.read().format("com.databricks.spark.xml")
            .option("rootTag", "PubmedArticleSet")
            .option("rowTag", "PubmedArticle")
            .load("s3://"+bucket+"/"+input);
        //df.printSchema();

        df.select("MedlineCitation.PMID",
                  "MedlineCitation.MeshHeadingList.MeshHeading.DescriptorName")
            .show();

        Column col = df.col("MedlineCitation.MeshHeadingList.MeshHeading");
        Dataset<Row> flatten = df.withColumn("MeshHeading",
                                             functions.explode(col));
        //flatten.printSchema();        

        flatten.select
            (flatten.col("MedlineCitation.PMID._VALUE").alias("PMID")
             ,flatten.col("MeshHeading.DescriptorName._UI").alias("MeSH")
             ,flatten.col("MeshHeading.DescriptorName._MajorTopicYN")
             .alias("MajorTopic")
             )
            .repartition(1) // write to a single file
            .write()
            .mode(SaveMode.Append)
            .format("csv")
            .save("s3://"+bucket+"/"+output);
    }
    
    static void process (SparkSession spark, String output, InputStream is)
        throws Exception {
        File tempfile = File.createTempFile("pubmed", ".xml");
        long size = Files.copy(is, tempfile.toPath(),
                               StandardCopyOption.REPLACE_EXISTING);
        Dataset<Row> df = spark.read().format("com.databricks.spark.xml")
            .option("rootTag", "PubmedArticleSet")
            .option("rowTag", "PubmedArticle")
            .load(tempfile.getPath());
        df.printSchema();

        df.select("MedlineCitation.PMID",
                  "MedlineCitation.MeshHeadingList.MeshHeading.DescriptorName")
            .show();

        Column col = df.col("MedlineCitation.MeshHeadingList.MeshHeading");
        Dataset<Row> flatten = df.withColumn("MeshHeading",
                                             functions.explode(col));
        flatten.printSchema();        

        flatten.select
            (flatten.col("MedlineCitation.PMID._VALUE").alias("PMID")
             ,flatten.col("MeshHeading.DescriptorName._UI").alias("MeSH")
             ,flatten.col("MeshHeading.DescriptorName._MajorTopicYN")
             .alias("MajorTopic")
             //,flatten.col("MeshHeading.QualifierName._UI").alias("Qualifiers")
             )
            .repartition(1) // write to a single file
            .write()
            .format("csv")
            .save(output);
    }
    
    public static void main(String[] argv) throws Exception {
        if (argv.length < 2) {
            System.err.println("Usage: SparkPubMed BUCKET INPUT...");
            System.exit(1);
        }

        SparkSession spark = SparkSession
            .builder()
            .appName(SparkPubMed.class.getName())
            .getOrCreate();

        AmazonS3 s3  = //new AmazonS3Client();
            AmazonS3ClientBuilder.defaultClient();

        String bucket = argv[0];
        spark.log().debug("### BUCKET="+bucket);
        
        for (int i = 1; i < argv.length; ++i) {
            String outpath = argv[i]+".out";
            File f = new File (argv[i]);
            if (f.exists()) {
                process (spark, outpath, new GZIPInputStream
                         (new FileInputStream (f)));
            }
            else {
                ObjectListing listing = s3.listObjects(bucket, argv[i]);
                do {
                    for (S3ObjectSummary sobj : listing.getObjectSummaries()) {
                        String key = sobj.getKey();
                        if (key.endsWith(".xml.gz")) {
                            processEMR (spark, sobj.getBucketName(),
                                        outpath, key);
                        }
                    }
                    listing = s3.listNextBatchOfObjects(listing);
                }
                while (listing.isTruncated());
            }
        }
        
        spark.stop();
        s3.shutdown();
    } // main
} // SparkPubMed
