import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col
import util.{OsmReader, SparkConnection}

object Extractor extends SparkConnection {
  def main(args: Array[String]): Unit = {

    // path to source files *.parquet
    val sourcePath = args.head

    // path to target files *.xlsx
    val targetPath = args.tail.head

    val spark = create(name = "OSM Extract data")

    import spark.implicits._

    // ids for test
    val ids: Seq[Long] = Seq(
      85606, // Ростовская область
      0
    )

    val nodes: DataFrame     = read(sourcePath + "//nodes", spark)
    val ways: DataFrame      = read(sourcePath + "//ways", spark)
    val relations: DataFrame = read(sourcePath + "//relations", spark)

    val resInfo = OsmReader.info(relations.select(col("ID"), col("TYPE"), col("TAG"), col("INFO")), ids).drop("TAG")
      .repartition(1).sortWithinPartitions("ID")

    val resTags  = OsmReader.tagsExplode(relations.select(col("ID"), col("TYPE"), col("TAG")), ids)
      .repartition(1).sortWithinPartitions("ID", "TAG_POS")

    val resRels  = OsmReader.relationsExplode(relations.select(col("ID"), col("TYPE"), col("RELATION")), ids)
      .repartition(1).sortWithinPartitions("ID", "RELATION_POS")

    val resWays  = OsmReader.waysExplode(ways.select(col("ID"), col("TYPE"), col("WAY")), resRels.select(col("RELATION_ID")).as[Long].collect.toSeq)
      .repartition(1).sortWithinPartitions("WAY_ID", "NODE_POS")

    val resNodes = OsmReader.nodes(nodes.select(col("ID"), col("TYPE"), col("LON"), col("LAT")), resWays.select(col("WAY_ID")).as[Long].collect.toSeq)
      .repartition(1).sortWithinPartitions("ID")

    writeExcel(resInfo, targetPath + "//info.xlsx")
    writeExcel(resTags, targetPath + "//tags.xlsx")
    writeExcel(resRels, targetPath + "//relations.xlsx")
    writeExcel(resWays, targetPath + "//ways.xlsx")
    writeExcel(resNodes, targetPath + "//nodes.xlsx")
  }

  def read(path: String, spark: SparkSession): DataFrame = {
    spark.read.format("parquet").load(path)
  }

  def writeExcel(df: DataFrame, path: String): Unit = {
    df.write.format("com.crealytics.spark.excel")
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("dateFormat", "yy-mm-dd")
      .option("timestampFormat", "yyyy-mm-dd hh:mm:ss.000")
      .save(path)
  }
}
