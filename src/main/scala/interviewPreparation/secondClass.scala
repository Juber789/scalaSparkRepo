package interviewPreparation

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object secondClass {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("secondClass").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

    // read data from csv file.
    val dataPath = "E:\\BigData\\datasets\\us-500.csv"
    val dataFrame = spark.read.format("csv").option("header","true").option("inferSchema","true").load(dataPath)
//    dataFrame.printSchema()
//    dataFrame.show(5,false)

    /*//run sql quries on top of dataFrame.
    val result = dataFrame.where($"state"==="NJ" && $"email".like("%gmail.com"))
    result.show(false)
    val stateCountRes = dataFrame.groupBy($"state").agg(count("*").alias("cont")).where($"state"==="NJ")
    stateCountRes.show(false)*/

    dataFrame.createOrReplaceTempView("usTab")
    val result = spark.sql("select * from usTab where state='NJ' and email like '%gmail.com'")
//    result.show(false)
    val stateCountRes = spark.sql("select state,count(*) as cont from usTab where state='NJ' group by state")
//    stateCountRes.show(false)

    // fetch list of columns from table.
    val list = List("first_name","address","county")
    val newColDataFrame = dataFrame.select(list.map(col): _*)
    newColDataFrame.show(6,false)

    //renaming columns
    val newCols = Map("first_name" -> "first_name_agent", "address" -> "address_agent", "state" -> "state_agent","zip" -> "zip_Agent")
    val aliasColumsDataFrame = dataFrame.select(newCols.map(x=>col(x._1).alias(x._2)).toList :_*)
    aliasColumsDataFrame.show(8,false)

    spark.stop()
  }
}