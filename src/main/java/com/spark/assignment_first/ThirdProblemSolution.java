package com.spark.assignment_first;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class ThirdProblemSolution {

	public static void main(String[] args) {
		//usingSparkSQL();
		useSparkRDD();
	}

	private static void usingSparkSQL() {

		SparkSession session = SparkSession.builder().appName("FirstProblemSolution").master("local[*]").getOrCreate();
		DataFrameReader dataFrameReader = session.read();
		Dataset<Row> trips = dataFrameReader.option("header", "true")
				.csv("E:\\BIGDATA\\Spark_Pig_Assignment\\Files\\*");

		Dataset<Row> ratingLean = trips.select(trips.col("payment_type"));
		Dataset<Row> groupedData = ratingLean.groupBy("payment_type").count();
		groupedData.show();

		Dataset<Row> sortedData = groupedData.sort(groupedData.col("payment_type"));
		sortedData.show();

		Dataset<Row> finalData = sortedData.select(groupedData.col("payment_type"));
		finalData.show();
	}

	private static void useSparkRDD() {

		SparkConf sparkConf = new SparkConf().setAppName("ThirdProblemSolution").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		JavaRDD<String> trips = sc.textFile("E:\\BIGDATA\\Spark_Pig_Assignment\\Files\\*");
		JavaPairRDD<String, Double> paymentTypeRDD = trips.mapToPair(data -> {
			List<String> dataList = new ArrayList<String>();
			dataList = Arrays.asList(data.split(","));
			if(dataList.size() > 10)
				return new Tuple2<String, Double>(dataList.get(9), 1.0);
			return new Tuple2<String, Double>("0", 0.0);
		});
		
		JavaPairRDD<String, Double> paymentTypeRDDGrouped = paymentTypeRDD.reduceByKey((x,y) -> x+y);
		List<Tuple2<String, Double>> list = paymentTypeRDDGrouped.takeOrdered(10, new TupleComparator());
				
		
		for (Tuple2<String, Double> tuple2 : list) {
			System.out.println(tuple2._1 + "---" + tuple2._2);
		}
		
		

	}
}
