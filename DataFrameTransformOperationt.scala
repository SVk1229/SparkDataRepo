package com.spark.scenarios

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

object DataFrameTransformOperation  extends App{
  
  val spark = SparkSession.builder()
                          .appName("STo")
                          .master("local[*]")
                          .getOrCreate()
                          
  import spark.implicits._
  val rdd1 = spark.sparkContext.textFile("C:/Users/34979/Desktop/Scinari.txt", 1)
                               .map(x => x.split(","))
                               .map(x => Row(x(0),x(1),x(2)))
                               
   val schema = StructType(Array(StructField("id",StringType,true)
       ,StructField("name",StringType,true),StructField("city",StringType,true)))
 
       
   val  df = spark.createDataFrame(rdd1, schema)
   
   df.show()
   
   /* +---+-------+----+
| id|   name|city|
+---+-------+----+
| id|   name|city|
|  1|    sai| blr|
|  2| venkat| hyd|
|  3|krishna| blr|
|  4| prasad| che|
|  5|    ram| blr|
+---+-------+----+
    */
   
   //val a = df.where($"city" !== "city")  || val a = df.filter($"city" !== "city")
   
   //val a = df.where(df.col("city").notEqual("city")) || val a = df.filter(df.col("city").notEqual("city")) 
   
    val purifiedDf = df.where($"city" !== "city")
   
    val getBlr = purifiedDf.withColumn("Fullname", when($"city"==="blr","Bengaluru")
                                                   .when($"city"==="hyd", "hyderabad")
                                                   .when($"city"==="che", "chennai")
                                                   .otherwise("NA"))
    /*  
   +---+-------+----+-------
| id|   name|city| Fullname|
+---+-------+----+---------+
|  1|    sai| blr|Bengaluru|
|  2| venkat| hyd|hyderabad|
|  3|krishna| blr|Bengaluru|
|  4| prasad| che|  chennai|
|  5|    ram| blr|Bengaluru|
+---+-------+----+---------+            
 */

   val func = udf((s:String) => if(s.equals("blr")) "Bengaluru" 
                                else if(s.equals("hyd")) "Hyderabad"
                                else if(s.equals("che")) "chennai"
                                else "NA")
   
   val getAdditioncol = getBlr.select($"id",$"name",$"city",$"Fullname", func($"city").as("DuplicateColumn"))
 /*  
   +---+-------+----+---------+-------------
| id|   name|city| Fullname|DuplicateColumn|
+---+-------+----+---------+---------------+
|  1|    sai| blr|Bengaluru|      Bengaluru|
|  2| venkat| hyd|hyderabad|      Hyderabad|
|  3|krishna| blr|Bengaluru|      Bengaluru|
|  4| prasad| che|  chennai|        chennai|
|  5|    ram| blr|Bengaluru|      Bengaluru|
+---+-------+----+---------+---------------+             
 */
   def getDataFramecityWithLength(s : DataFrame) : DataFrame = {

		   val a = udf((s:String) => s.length())

				   s.select($"*",a($"Fullname").as("LengthOfCity"))
  }
   
  val newDfFromExistingDF = getDataFramecityWithLength(getAdditioncol)
  
 // newDfFromExistingDF.show()
  /*
   * +---+-------+----+---------+---------------+------------+
| id|   name|city| Fullname|DuplicateColumn|LengthOfCity|
+---+-------+----+---------+---------------+------------+
|  1|    sai| blr|Bengaluru|      Bengaluru|           9|
|  2| venkat| hyd|hyderabad|      Hyderabad|           9|
|  3|krishna| blr|Bengaluru|      Bengaluru|           9|
|  4| prasad| che|  chennai|        chennai|           7|
|  5|    ram| blr|Bengaluru|      Bengaluru|           9|
+---+-------+----+---------+---------------+------------+
   */
 
  def addStateToDuplicate(s : DataFrame) : DataFrame = {
    
    val func = udf((s :String) => if(s.equals("Bengaluru")) "KA-"+"Bengaluru"
                                  else if(s.equals("Hyderabad")) "TS-"+"Hyderabad"
                                  else if(s.equals("chennai")) "TN-"+"chennai"
                                  else "NA")
    
    s.select($"id",$"name",$"city",$"Fullname",func($"DuplicateColumn").as("DuplicateColumn"))
  }
  
  val finalresult = addStateToDuplicate(newDfFromExistingDF)
  
  finalresult.show()
}
