package pubmed;

import java.io.*;
import java.nio.file.*;
import java.util.*;

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

public class SparkMesh {
    public static class TreeNumberToNameMapping
        implements Serializable, Function<Row, Row> {
        static final MeshLookup mesh = new MeshLookup ();
        public TreeNumberToNameMapping () {
        }

        public Row call (Row row) {
            String name = mesh
                .getDescriptorNameFromTreeNumber(row.getString(0));
            return RowFactory.create // treeNumber,name,count
                (row.getString(0), name, row.getLong(1));
        }
    }
    
    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            System.err.println("Usage: pubmed.SparkMesh BUCKET NAME");
            System.exit(1);
        }
        
        SparkSession spark = SparkSession
            .builder()
            .appName(SparkMesh.class.getName())
            .getOrCreate();

        Dataset<Row> df = spark.read().format("csv")
            .load("s3://"+argv[0]+"/"+argv[1]+"/*.csv");
        //System.out.println("########## total rows = "+df.count());
        df.show(100);
        
        StructType schema = new StructType ()
            .add("TreeNumber", DataTypes.StringType)
            .add("Name", DataTypes.StringType)
            .add("Count", DataTypes.LongType);

        df = df.filter(df.col("_c3").startsWith("A11."));
        df.show(100);        
        df.createOrReplaceTempView("mesh");

        df = spark.createDataFrame
            (spark.sql("select _c3 as TreeNumber, "
                       +"count(distinct _c0) as Count from mesh "
                       +"group by _c3")
             .javaRDD().map(new TreeNumberToNameMapping ()), schema);

        df.sort(df.col("Count").desc())
            .repartition(1)
            .write()
            .mode(SaveMode.Append)
            .format("csv")
            .save("s3://"+argv[0]+"/"+argv[1]+".A11");
        
        spark.stop();
    }
}
