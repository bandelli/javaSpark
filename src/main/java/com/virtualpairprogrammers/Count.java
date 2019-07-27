package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class Count {

    public static void main(String[] args) {
         List<Integer> inputdata = new ArrayList<>();
         inputdata.add(35);
         inputdata.add(12);
         inputdata.add(90);
         inputdata.add(20);

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> myRDD = sc.parallelize(inputdata);

        Integer result = myRDD.reduce((value1, value2) -> value1 + value2);

        JavaRDD<Double> sqrtRdd = myRDD.map(value -> Math.sqrt(value));

        sqrtRdd.collect().forEach(System.out::println);

        //quantos elementos existem no sqrtrdd
        System.out.println(sqrtRdd.count());

        //usando map reduce
        JavaRDD<Long> singleIntegerRdd = sqrtRdd.map( value -> 1L);
        Long count = singleIntegerRdd.reduce((value1, value2) -> value1 + value2);
        System.out.println("Função Map Reduce");
        System.out.println(count);

        System.out.println(result);

        sc.close();
    }
}