import org.apache.spark.sql.SparkSession

object example2 extends App {
  val sparkSession = SparkSession
    .builder()
    .appName("Oralce Query 1")
    .master("local[4]")
    .getOrCreate()

  val spark = sparkSession.sqlContext

  //read json file into dataframe
  val df = spark.read.json("C:/Users/84974/Downloads/example.json")
  df.printSchema()
  df.show(false)

}
