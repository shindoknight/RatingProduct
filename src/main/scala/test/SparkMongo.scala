package test
import com.mongodb.spark.{MongoConnector, MongoSpark}
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.{SparkConf, SparkContext}
object SparkMongo {

  def main(args: Array[String]): Unit = {

    val sc =SparkContext()
    val readConf1 = ReadConfig(Map("uri" -> "mongodb://127.0.0.1/data.detail?readPreference=primaryPreferred"))
    val readConf2 = ReadConfig(Map("uri" -> "mongodb://127.0.0.1/data.detail?readPreference=primaryPreferred"))
    val writeConf = WriteConfig(Map("uri" -> "mongodb://127.0.0.1/data.output"))
    val versiondf=MongoSpark.load(sc,readConf2).toDF()
    val dff = MongoSpark.load(sc,readConf1).toDF()
    val maxver=versiondf.agg(max(col("ProductVersion")).as("ProductVersion")).head().getValuesMap[Long](Seq("ProductVersion")).get("ProductVersion").getOrElse(0)
    val df= dff.filter($"ProductVersion"===maxver)
    def getCategory (url: String):String={
      return url.split("/").last
    }
    val getCateUDF = udf[String, String] (getCategory)
    val dataF=df.withColumn("RatingCount",$"AggregateRating.ratingCount".cast("float")).withColumn("Category",getCateUDF(col("CategoryURL")))
    val windowSpec = Window.partitionBy("Category")
    val maxDF = dataF.withColumn("maxRatingCount", max(col("RatingCount")).over(windowSpec))

    def getRank(ratingcount: Float, max: Float ): Int ={
      if (ratingcount.isNaN) return 0
      val req= (ratingcount/max*5).toInt
      if(req>4) return 5
      if(req==0) return 1
      else return req
    }

    val getRankUDF = udf[Int, Float, Float] (getRank)
    val rankDF=maxDF.withColumn("MarkRating",getRankUDF($"RatingCount",$"maxRatingCount")).na.fill(0,Seq("MarkRating"))
    val rankDF2= rankDF.withColumn("MinMark",min("MarkRating").over(windowSpec)).withColumn("MaxMark",max("MarkRating").over(windowSpec)).withColumn("AvgMark",avg("MarkRating").over(windowSpec))
    val result= rankDF2.select("_id.oid","CategoryURL","MarkRating","MinMark", "MaxMark", "AvgMark")

    MongoSpark.save(result.write.mode("overwrite"),writeConf)
}
