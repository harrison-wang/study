package com.wfz.study.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.spark.SparkConf;

public class SparkTest {

	public static void testParallelizedCollection() {

		SparkConf conf = new SparkConf().setAppName("wfz_spark").setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> rdd = sc.parallelize(list);
		int result = rdd.reduce(new Function2<Integer, Integer, Integer>() {

			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}

		});

		System.out.println(result);

	}
	
	public static void testHdfs() {
		SparkConf conf = new SparkConf().setAppName("wfz_spark").setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> rdd = sc.textFile("hdfs://localhost:9000/ncdc/1901");
		String result = rdd.reduce(new Function2<String, String, String>() {

			public String call(String v1, String v2) throws Exception {
				return v1 + v2;
			}

		});

		System.out.println(result);
	}
	
	public static void testHFile() {
		SparkConf conf = new SparkConf().setAppName("wfz_spark").setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> rdd = sc.textFile("hdfs://localhost:9000/ncdc/1901");
		JavaPairRDD<String, Integer> pairRDD = rdd.mapToPair(new PairFunction<String, String, Integer>() {

			public Tuple2<String, Integer> call(String t) throws Exception {
				return null;
			}  
        });
		String  hfilePath = "hdfs://localhost:9000/ncdc/1901.hfile";
		Configuration hbaseConf = HBaseConfiguration.create();  
        String zk = "127.0.0.1:2181";  
        String tableName = "blog";  
        hbaseConf.set("hbase.zookeeper.quorum", zk);  
		try {
			HTable table = new HTable(hbaseConf, tableName);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
		hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName); 
		pairRDD.saveAsNewAPIHadoopFile(hfilePath, String.class, Integer.class, HFileOutputFormat.class, hbaseConf);  
	}

	public static void main(String[] args) {
		testHFile();
	}

}
