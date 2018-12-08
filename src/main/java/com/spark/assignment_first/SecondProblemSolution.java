package com.spark.assignment_first;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SecondProblemSolution {

	public static void main(String[] args) {
		useSparkRDD();
	}

	public static void useSparkSQL() {

		SparkSession session = SparkSession.builder().appName("SecondProblemSolution").master("local[*]").getOrCreate();
		DataFrameReader dataFrameReader = session.read();
		Dataset<Row> trips = dataFrameReader.option("header", "true")
				.csv("E:\\BIGDATA\\Spark_Pig_Assignment\\Files\\*");

		Dataset<Row> rateCodeIDFilteredTrips = trips.filter(trips.col("RatecodeID").$eq$eq$eq(4));
		rateCodeIDFilteredTrips.show();
		System.out.println(rateCodeIDFilteredTrips.count());

		int counter = 0;
		for (Row row : rateCodeIDFilteredTrips.collectAsList()) {
			counter++;
			System.out.println("======>>>>> " + row);
		}
		System.out.println("======>>>>> " + counter);

	}

	public static void useSparkRDD() {

		SparkConf sparkConf = new SparkConf().setAppName("ThirdProblemSolution").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		JavaRDD<String> trips = sc.textFile("E:\\BIGDATA\\Spark_Pig_Assignment\\Files\\*");
		//JavaRDD<String> trips = sc.textFile("E:\\BIGDATA\\Spark_Pig_Assignment\\sample_files\\*");

		JavaRDD<String> filtered = trips.filter(data -> data.split(",").length > 10 && data.split(",")[9].equals("4"));

		for (String element : filtered.collect()) {
			System.out.println(element);
		}
	}

}
