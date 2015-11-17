import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Created by Michael on 11/5/15.
 */
public class Histogram_ratings {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("Histogram-Ratings").setMaster("local[1]");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        java.sql.Timestamp startTimestamp = new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());
        long startTime = System.nanoTime();
        JavaRDD<String> lines = ctx.textFile("../Classification/file1", 1);

        JavaRDD<Integer> ratings = lines.flatMap(new FlatMapFunction<String, Integer>() {
            @Override
            public Iterable<Integer> call(String s) throws Exception {
                List<Integer> intList = new ArrayList<Integer>();
                int rating, reviewIndex, movieIndex;
                String reviews = new String();
                String tok = new String();
                String ratingStr = new String();

                movieIndex = s.indexOf(":");
                if (movieIndex > 0) {
                    reviews = s.substring(movieIndex + 1);
                    StringTokenizer token = new StringTokenizer(reviews, ",");
                    while (token.hasMoreTokens()) {
                        tok = token.nextToken();
                        reviewIndex = tok.indexOf("_");
                        ratingStr = tok.substring(reviewIndex + 1);
                        rating = Integer.parseInt(ratingStr);
                        intList.add(rating);
                    }
                }
                return intList;
            }
        });

        JavaPairRDD<Integer, Integer> ones = ratings.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                return new Tuple2<Integer, Integer>(integer, 1);
            }
        });

        JavaPairRDD<Integer, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        long endTime = System.nanoTime();
        java.sql.Timestamp endTimestamp = new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());

        System.out.println("This job started at " + startTimestamp);
        System.out.println("This job finished at: " + endTimestamp);
        System.out.println("The job took: " + (endTime - startTime)/1000000 + " milliseconds to finish");

        // To display the result
        List<Tuple2<Integer, Integer>> output = counts.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        //

        ctx.stop();

    }
}