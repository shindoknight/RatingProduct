package test

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig

object SparkCommon {

  val colEmployee = "detail"

  val mongoUri = "mongodb://localhost:27017/detail." + colEmployee

  lazy val conf = new SparkConf()
  conf.setAppName("Spark-Mongo")
    .setMaster("local")
    .set("spark.mongodb.input.uri", mongoUri)
    .set("spark.mongodb.output.uri", mongoUri)

  lazy val sparkCtx = new SparkContext(conf)

  lazy val sqlCtx = new SQLContext(sparkCtx)


  def getSparkSession(args: Array[String]): SparkSession = {
    val uri: String = args.headOption.getOrElse("mongodb://localhost/test.coll")

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("MongoSparkConnectorTour")
      .set("spark.app.id", "MongoSparkConnectorTour")
      .set("spark.mongodb.input.uri", uri)
      .set("spark.mongodb.output.uri", uri)

    val session = SparkSession.builder().config(conf).getOrCreate()
    MongoConnector(session.sparkContext).withDatabaseDo(WriteConfig(session), {db => db.drop()})
    session
  }
}