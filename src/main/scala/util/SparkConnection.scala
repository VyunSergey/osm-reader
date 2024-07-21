package util

import org.apache.spark.sql.SparkSession

trait SparkConnection {
  def create(name: String = "OsmReader"): SparkSession =
    SparkSession
      .builder()
      .appName(name)
      .config("spark.master", "local[16]")
      .config("spark.driver.memory", "8gb")
      .config("spark.driver.maxResultSize", "8gb")
      .config("spark.executor.memory", "8gb")
      .getOrCreate()

}
