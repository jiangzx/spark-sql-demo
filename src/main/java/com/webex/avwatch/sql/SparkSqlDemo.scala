package com.webex.avwatch.sql

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils

object SparkSqlDemo {
  def main(args: Array[String]) = {
    System.setProperty("hadoop.home.dir", "C:/hadoop")
    val conf = new SparkConf().setAppName("SparksqlLoadDat Application").setMaster("local")
    val sc = new SparkContext(conf)

    val sqlc = new SQLContext(sc)
    // val p = sc.textFile("src/test/resources/log_2017_03_24_005558.log_analyzed.dat")
    val p = sc.textFile("hdfs://10.224.243.130:8020/user/admin/cusp/2017-03-24/log_2017_03_24_005558.log_analyzed.dat")
    val pmap = p.map(p => p.split(","))

    val RecordRDD = pmap.map(p => CuspRaw(p(0), p(1), p(2), p(3)))

    import sqlc.implicits._

    val RecordDF = RecordRDD.toDF()
    RecordDF.registerTempTable("CUSP_RAW")

    // print(sqlc.sql("select * from CUSP_RAW").collect().toList)
    
    val result = sqlc.sql("select * from CUSP_RAW")
    
//    val prop = new java.util.Properties
//    val url = "jdbc:oracle:thin:@(DESCRIPTION = (ADDRESS_LIST = (ADDRESS = (PROTOCOL = TCP)(HOST = sjdbavwatch-scan.webex.com)(PORT = 1521))) (CONNECT_DATA = (SERVICE_NAME = avwatchha.webex.com) (SERVER = DEDICATED)))"
//    prop.setProperty("user","avwatch")
//    prop.setProperty("password","qBoaLoLHE39xDgox0V")
//    Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()
//    JdbcUtils.saveTable(result, url, "AVWRPT.TEST_CUSP_ORC", prop)
    
    
    
    // JdbcUtils.saveTable(df, url, table, properties)
    //    var myList = Array(1.9, 2.9, 3.4, 3.5)
    //    for(x <- myList){
    //      println(x)
    //    }

  }
}