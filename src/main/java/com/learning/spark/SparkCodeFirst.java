package com.learning.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SparkCodeFirst {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("SparkCodeFirst").setMaster("local[1]");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		JavaRDD<Integer> javaRDD = jsc.parallelize(Arrays.asList(new Integer[] { 1, 2, 3, 4, 5, 6 }));

		// Print using "for each"
		javaRDD.foreach(x -> System.out.println(x));

		System.out.println("Using Collect......");
		for (Integer string : javaRDD.collect()) {
			System.out.println("==>>> " + string);
		}
		;

		JavaRDD<Integer> squareRDD = javaRDD.map(x -> x * x);
		System.out.println("Using Collect......");
		for (Integer string : squareRDD.collect()) {
			System.out.println("====>>> " + string);
		}
		;

		// Filter Operations
		JavaRDD<Integer> filterEvenRDD = javaRDD.filter(x -> x % 2 == 0 ? true : false);
		System.out.println("Filter Even using Collect......");
		for (Integer string : filterEvenRDD.collect()) {
			System.out.println("=====>>>> " + string);
		};

		// Filter using Old API
		JavaRDD<Integer> filterOddRDD = javaRDD.filter(new Function<Integer, Boolean>() {
			@Override
			public Boolean call(Integer v1) throws Exception {
				if (v1 % 2 != 0)
					return true;
				else
					return false;
			}
		});
		
		System.out.println("Filter Odd using Collect......");
		for (Integer string : filterOddRDD.collect()) {
			System.out.println("======>>>>> " + string);
		};
	}

}
