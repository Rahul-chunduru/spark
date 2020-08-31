package sparkTest;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCount {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// configure spark
        SparkConf sparkConf = new SparkConf().setAppName("Read Text to RDD")
                                        .setMaster("local")
                                        .set("spark.executor.memory","2g");
        // start a spark context
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        
     // provide path to input text file
        String path = "/home/rahul/Desktop/sample.txt";
        
        // read text file to RDD
        JavaRDD<String> lines = sc.textFile(path);
        
        // flatMap each line to words in the line
        JavaPairRDD<String, Integer> words = lines.flatMap(s -> Arrays.asList(s.split(" "))
        		.iterator())
        		.mapToPair(w -> new Tuple2<>(w, 1))
        		.reduceByKey((a, b) -> a + b);
       
        
        // collect RDD for printing
        for(Tuple2<String, Integer> item_count:words.collect()){
            System.out.println(item_count);
        }
        
        sc.close();
	}

}
