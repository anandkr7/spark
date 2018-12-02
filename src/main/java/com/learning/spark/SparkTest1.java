package com.learning.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkTest1 {
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\winutils");
		SparkConf conf = new SparkConf().setAppName("MyFirstProgram").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<Integer> myRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
		myRDD = myRDD.map(x -> x % 2 == 0 ? x * x : x * x * x);

		System.out.println(myRDD.first());

		myRDD = myRDD.filter(x -> x > 100 && x < 500 ? true : false);

		System.out.print(myRDD.count());

		List<Integer> list = myRDD.collect();
		for (int i : list) {
			System.out.println(i);
		}

		JavaRDD<String> stringRDD = sc.parallelize(Arrays.asList("Pig", "Hadoop", "Lambda", "Java", "Hive", "Hue"));
		stringRDD = stringRDD
				.filter(X -> X.equals("Hadoop") ? true : (X.equals("Hive") ? true : (X.equals("Hue") ? true : false)));
		List<String> strList = stringRDD.collect();
		for (String i : strList) {
			System.out.println(i);
		}

		stringRDD =	stringRDD.filter(X -> {
			if (X.equals("Hadoop"))
				return true;
			else if (X.equals("Hive"))
				return true;
			else if (X.equals("Hue"))
				return true;
			else
				return false;
		});
		strList = stringRDD.collect();
		for (String i : strList) {
			System.out.println(i);
		}
		
		stringRDD =	stringRDD.filter(X -> X.startsWith("H"));
		strList = stringRDD.collect();
		for (String i : strList) {
			System.out.println(i);
		}
		
		sc.close();
	}
}
