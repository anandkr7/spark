package com.learning.spark;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataRatingsSpark {

	public static void main(String[] args) {
		
		SparkSession session = SparkSession.builder().appName("Top20Movies").master("local[*]").getOrCreate();
		DataFrameReader dataFrameReader = session.read();
		Dataset<Row> ratings = dataFrameReader.option("header", "true").csv("C:\\Users\\Anand\\eclipse-workspace\\spark\\src\\main\\resources\\in\\in.data");
		
		Dataset<Row> ratingLean = ratings.select(ratings.col("UserID"));
		Dataset<Row> groupedData = ratingLean.groupBy("UserID").count();
		Dataset<Row> sortedData =groupedData.select(groupedData.col("UserID"));
		sortedData.show();
		
		Dataset<Row> movieRatings = ratings.select(ratings.col("UserID"), ratings.col("Rating"));
		Dataset<Row> filteredData = movieRatings.filter(movieRatings.col("Rating").$less(5));
		filteredData.show();
		System.out.println(filteredData.count());
		
	}
	
}
