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
import org.apache.spark.sql.types.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;

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

public class SparkPubMed  {    
    public static class TreeNumberMapping
        implements Serializable, Function<Row, Row> {
        static final MeshLookup mesh = new MeshLookup ();
        public TreeNumberMapping () {
        }

        public Row call (Row row) {
            String ui = row.getString(1);
            String[] tr = mesh.getTreeNumbers(ui);
            if (tr != null) {
                Set<String> paths = new TreeSet<>();
                for (String p : tr) {
                    String[] nodes = p.split("\\.");
                    StringBuilder path = new StringBuilder (nodes[0]);
                    for (int i = 1; i < nodes.length; ++i) {
                        paths.add(path.toString());
                        path.append("."+nodes[i]);
                    }
                    paths.add(path.toString());
                }
                tr = paths.toArray(new String[0]);
            }
            return RowFactory.create
                (row.getLong(0), ui, row.getString(2), tr);
        }
    }

    final SparkSession spark;
    
    public SparkPubMed (SparkSession spark) {
        this.spark = spark;
        /*
        mesh = spark.sparkContext().broadcast
            (new MeshLookup (),
             scala.reflect.ClassTag$.MODULE$.apply(MeshLookup.class));
        */
    }

    Dataset<Row> process (Dataset<Row> df, String output) throws Exception {
        //df.printSchema();
        /*
        df.select("MedlineCitation.PMID",
                  "MedlineCitation.MeshHeadingList.MeshHeading.DescriptorName")
            .show();
        */
        
        Column col = df.col("MedlineCitation.MeshHeadingList.MeshHeading");
        Dataset<Row> flatten = df.withColumn("MeshHeading",
                                             functions.explode(col));
        //flatten.printSchema();        

        StructType schema = new StructType ()
            .add("PMID", DataTypes.LongType)
            .add("MeSH", DataTypes.StringType)
            .add("MajorTopic", DataTypes.StringType)
            .add("TreeNumber", DataTypes.createArrayType
                 (DataTypes.StringType), true);
        
        flatten.createOrReplaceTempView("pubmed");

        df = spark.createDataFrame
            (spark.sql
             ("select MedlineCitation.PMID._VALUE as pmid, "
              +"MeshHeading.DescriptorName._UI as MeSH, "
              +"MeshHeading.DescriptorName._MajorTopicYN as MajorTopic "
              +"from pubmed")
             .javaRDD()
             .map(new TreeNumberMapping ()), schema);
        df = df.withColumn("TreeNumber",
                           functions.explode(df.col("TreeNumber")));
        
        //System.out.println("############ count = "+df.count());
        //df.printSchema();
        //df.show(100);

        df//.repartition(1)
            .write()
            .mode(SaveMode.Append)
            .format("csv")
            .save(output);
        
        return df;
    }
    
    public void processEMR (String bucket, String output, String input)
        throws Exception {
        Dataset<Row> df = spark.read().format("com.databricks.spark.xml")
            .option("rootTag", "PubmedArticleSet")
            .option("rowTag", "PubmedArticle")
            .load("s3://"+bucket+"/"+input+"/*.xml.gz");
        process (df, "s3://"+bucket+"/"+output);
    }
    
    public void process (String output, InputStream is) throws Exception {
        File tempfile = File.createTempFile("pubmed", ".xml");
        long size = Files.copy(is, tempfile.toPath(),
                               StandardCopyOption.REPLACE_EXISTING);
        Dataset<Row> df = spark.read().format("com.databricks.spark.xml")
            .option("rootTag", "PubmedArticleSet")
            .option("rowTag", "PubmedArticle")
            .load(tempfile.getPath());
        process (df, output);
    }
    
    public static void main(String[] argv) throws Exception {
        if (argv.length < 2) {
            System.err.println("Usage: SparkPubMed BUCKET INPUT [OUTPUT]");
            System.exit(1);
        }

        SparkSession spark = SparkSession
            .builder()
            .appName(SparkPubMed.class.getName())
            .getOrCreate();
        SparkPubMed pubmed = new SparkPubMed (spark);

        AmazonS3 s3  = //new AmazonS3Client();
            AmazonS3ClientBuilder.defaultClient();

        String bucket = argv[0];
        spark.log().debug("### BUCKET="+bucket);
        
        File f = new File (argv[1]);
        if (f.exists()) {
            pubmed.process(bucket, new GZIPInputStream
                           (new FileInputStream (f)));
        }
        else {
            String outpath = argv.length < 3 ? argv[1]+".msh" : argv[2];
            pubmed.processEMR(bucket, outpath, argv[1]);
        }
        
        spark.stop();
        s3.shutdown();
    } // main
} // SparkPubMed
