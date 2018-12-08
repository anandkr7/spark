package com.spark.assignment_first;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class FirstProblemSolution {

	public static void main(String[] args) {

		//useSparkSQL();
		useSparkRDD();
	}

	public static void useSparkSQL() {

		SparkSession session = SparkSession.builder().appName("FirstProblemSolution").master("local[*]").getOrCreate();
		DataFrameReader dataFrameReader = session.read();
		Dataset<Row> trips = dataFrameReader.option("header", "true")
				.csv("E:\\BIGDATA\\Spark_Pig_Assignment\\Files\\*");

		Dataset<Row> filteredTrips = trips.filter(trips.col("VendorID").$eq$eq$eq(2)
				.and(trips.col("tpep_pickup_datetime").$eq$eq$eq("2017-10-01 00:15:30"))
				.and(trips.col("tpep_dropoff_datetime").$eq$eq$eq("2017-10-01 00:25:11"))
				.and(trips.col("passenger_count").$eq$eq$eq(1)).and(trips.col("trip_distance").$eq$eq$eq("2.17")));

		filteredTrips.show();
		System.out.println(filteredTrips.count());

	}

	public static void useSparkRDD() {
		
		SparkConf sparkConf = new SparkConf().setAppName("ThirdProblemSolution").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		//JavaRDD<String> trips = sc.textFile("E:\\BIGDATA\\Spark_Pig_Assignment\\Files\\*");
		JavaRDD<String> trips = sc.textFile("E:\\BIGDATA\\Spark_Pig_Assignment\\sample_files\\*");
		
		JavaRDD<String> filtered = trips.filter(data -> 
			data.split(",")[0].equals("2") 
			&& data.split(",")[1].equals("2017-10-01 00:15:30")
			&& data.split(",")[2].equals("2017-10-01 00:25:11")
			&& data.split(",")[3].equals("1")
			&& data.split(",")[4].equals("2.17"));
		
		for (String element : filtered.collect()) {
			System.out.println(element);
		}
	}

}
