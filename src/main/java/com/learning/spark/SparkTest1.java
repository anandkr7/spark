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
		
		sc.close();
	}
}
