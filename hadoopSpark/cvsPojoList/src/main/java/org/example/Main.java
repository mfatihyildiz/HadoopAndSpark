package org.example;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.util.List;

public class Main {
    // Map spark dataset to list of POJO objects, in the form of list of objects.
    //
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("SparkCSV")
                .master("local[*]")
                .getOrCreate();

        // Reading the csv file from hdfs
        //
        Dataset<Row> res = sparkSession.read()
                .option("header", "true")
                .csv("hdfs://127.0.0.1:9000/user/fatih/emp.csv");
        res.show();
        // Encoder for model class
        //
        Encoder<Emp> empEncoder = Encoders.bean(Emp.class);
        // Make dataset Emp object list.
        //
        List<Emp> emp = res.map(new MapFunction<Row, Emp>() {
            @Override
            public Emp call(Row val) throws Exception {
                return new Emp(
                        Integer.parseInt(val.getString(0)),
                        val.getString(1),
                        val.getString(2),
                        (val.getString(3) != null) ? Integer.parseInt(val.getString(3)) : null,
                        val.getString(4),
                        Integer.parseInt(val.getString(5)),
                        (val.getString(6) != null) ? Integer.parseInt(val.getString(6)) : null,
                        Integer.parseInt(val.getString(7))
                );
            }
        }, empEncoder).collectAsList();
        

        Emp e = emp.get(0);
        System.out.println(e.toString());
    }
}