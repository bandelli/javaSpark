package com.javaspark.estudos;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class JoinDF {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("SparkJoinDF").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer, Integer>> visitsRaw = new ArrayList<>();
        visitsRaw.add(new Tuple2<>(4, 18));
        visitsRaw.add(new Tuple2<>(6, 4));
        visitsRaw.add(new Tuple2<>(10, 9));

        List<Tuple2<Integer, String>> userRaw = new ArrayList<>();
        userRaw.add(new Tuple2<>(1, "Mariana"));
        userRaw.add(new Tuple2<>(2, "Thais"));
        userRaw.add(new Tuple2<>(3, "Vicky"));
        userRaw.add(new Tuple2<>(4, "Pati"));
        userRaw.add(new Tuple2<>(5, "Tony"));
        userRaw.add(new Tuple2<>(6, "Tera"));


        JavaPairRDD<Integer, Integer> visitas = sc.parallelizePairs(visitsRaw);
        JavaPairRDD<Integer, String> usuarios = sc.parallelizePairs(userRaw);

        //JavaPairRDD<Integer, Tuple2<Integer, String>> joinedRdd = visitas.join(usuarios);
        //JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> joinedRdd = visitas.leftOuterJoin(usuarios); //pega os valores nulos
        JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> joinedRdd = visitas.rightOuterJoin(usuarios); //pega os valores nulos

        //o collect foi usado apenas por teste de execução em maquina local.  //segundo valor e segundo valor da tupla
        //joinedRdd.collect().forEach( elemento -> System.out.println(elemento._2._2.orElse("branco").toUpperCase()));
        joinedRdd.collect().forEach( elemento -> System.out.println(" user " + elemento._2._2 + " had " + elemento._2._1.orElse(0)));

        sc.close();

    }
}
