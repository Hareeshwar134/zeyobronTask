package sparktaskPack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.time.{ZonedDateTime, ZoneId}
import java.time.format.DateTimeFormatter

object taskObj {
  
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("First").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark=SparkSession.builder().getOrCreate()
		import spark.implicits._
    
    /*println("-----rawdata----")
    
    val data=sc.textFile("file:///C:/data/txns")
    
    data.take(10).foreach(println)
    
    println("----gymdata----")
    
    val gymdata=data.filter(x=>x.contains("Gymnastics"))
    
    gymdata.take(10).foreach(println)
    
    gymdata.saveAsTextFile("file:///E:/data/gymdata_dir_zeyo")
    
    val nulldata = spark.read.format("csv").option("header","true").option("inferschema","true").load("file:///E:/data/Nulldata01.csv")
    nulldata.show()
    nulldata.printSchema()
    
    nulldata.na.fill(0).na.fill("NA")show()*/
    
		val yesterday = ZonedDateTime.now(ZoneId.of("UTC")).minusDays(1)
		val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm'Z'")
		val result = formatter format yesterday
		
  }
}