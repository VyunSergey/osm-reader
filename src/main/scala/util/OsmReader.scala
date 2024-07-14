package util

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.wolt.osm.spark.OsmSource.OsmSource

object OsmReader {
  def osm(path: String, spark: SparkSession): DataFrame = {
    spark.read
      .option("threads", 18)
      .option("partitions", 64)
      .format(OsmSource.OSM_SOURCE_NAME)
      .load(path)
  }

  def nodes(osm: DataFrame, ids: Seq[Long] = Nil): DataFrame = {
    val nodes: DataFrame = osm.where(col("TYPE") === 0)
    if (ids.nonEmpty) nodes.where(col("ID").isin(ids: _*)) else nodes
  }

  def ways(osm: DataFrame, ids: Seq[Long] = Nil): DataFrame = {
    val ways: DataFrame = osm.where(col("TYPE") === 1)
    if (ids.nonEmpty) ways.where(col("ID").isin(ids: _*)) else ways
  }

  def waysExplode(osm: DataFrame, ids: Seq[Long] = Nil): DataFrame = {
    val df: DataFrame = ways(osm, ids)
    df.select(
      df.columns.map(col) :+
        posexplode_outer(col("WAY")).as(Seq("NODE_POS", "NODE_ID")): _ *)
      .withColumnRenamed("ID", "WAY_ID")
      .withColumnRenamed("TYPE", "WAY_TYPE")
      .drop("WAY")
  }

  def relations(osm: DataFrame, ids: Seq[Long] = Nil): DataFrame = {
    val rels: DataFrame = osm.where(col("TYPE") === 2)
    if (ids.nonEmpty) rels.where(col("ID").isin(ids: _*)) else rels
  }

  def relationsExplode(osm: DataFrame, ids: Seq[Long] = Nil): DataFrame = {
    val df: DataFrame = relations(osm, ids)
    df.select(
        df.columns.map(col) :+
          posexplode_outer(col("RELATION")).as(Seq("RELATION_POS", "RELATION_STRUCT")): _ *)
      .withColumn("RELATION_ID",   col("RELATION_STRUCT.ID"))
      .withColumn("RELATION_ROLE", col("RELATION_STRUCT.ROLE"))
      .withColumn("RELATION_TYPE", col("RELATION_STRUCT.TYPE"))
      .drop("RELATION", "RELATION_STRUCT")
  }

  def tagsExplode(osm: DataFrame, ids: Seq[Long] = Nil): DataFrame = {
    val df: DataFrame = if (ids.nonEmpty) osm.where(col("ID").isin(ids: _*)) else osm
    df.select(
      df.columns.map(col) :+
        posexplode_outer(col("TAG")).as(Seq("TAG_POS", "TAG_KEY", "TAG_VALUE")): _ *)
      .drop("TAG")
  }

  def name(osm: DataFrame, ids: Seq[Long] = Nil): DataFrame = {
    val df: DataFrame = if (ids.nonEmpty) osm.where(col("ID").isin(ids: _*)) else osm
    val dfName: DataFrame = tagsExplode(osm ,ids)
      .groupBy(col("ID"))
      .agg(
        max(when(lower(trim(col("TAG_KEY"))) === "name",         col("TAG_VALUE"))).as("NAME"),
        max(when(lower(trim(col("TAG_KEY"))) === "name:ru",      col("TAG_VALUE"))).as("NAME_RU"),
        max(when(lower(trim(col("TAG_KEY"))) === "name:en",      col("TAG_VALUE"))).as("NAME_EN"),
        max(when(lower(trim(col("TAG_KEY"))) === "addr:country", col("TAG_VALUE"))).as("ADDR_COUNTRY"),
        max(when(lower(trim(col("TAG_KEY"))) === "border_type",  col("TAG_VALUE"))).as("BORDER_TYPE"),
        max(when(lower(trim(col("TAG_KEY"))) === "admin_level",  col("TAG_VALUE"))).as("ADMIN_LEVEL"),
        max(when(lower(trim(col("TAG_KEY"))) === "boundary",     col("TAG_VALUE"))).as("BOUNDARY")
      )

    df.join(dfName, Seq("ID"), "inner")
  }

  def info(osm: DataFrame, ids: Seq[Long] = Nil): DataFrame = {
    val df: DataFrame = if (ids.nonEmpty) osm.where(col("ID").isin(ids: _*)) else osm

    name(df)
      .withColumn("INFO_UID",   col("INFO.UID"))
      .withColumn("INFO_USERNAME", col("INFO.USERNAME"))
      .withColumn("INFO_VERSION", col("INFO.VERSION"))
      .withColumn("INFO_TIMESTAMP", col("INFO.TIMESTAMP"))
      .withColumn("INFO_CHANGESET", col("INFO.CHANGESET"))
      .withColumn("INFO_VISIBLE", col("INFO.VISIBLE"))
      .drop("INFO")
  }

  def extract(osm: DataFrame, ids: Seq[Long] = Nil): DataFrame = {
    extract(rels = relations(osm, ids), ways = ways(osm), nodes = nodes(osm), ids)
  }

  def extract(rels: DataFrame, ways: DataFrame, nodes: DataFrame, ids: Seq[Long]): DataFrame = {
    val relsFlt = relations(rels, ids)
    val relsExp = relationsExplode(relsFlt, ids).select(col("ID"), col("TYPE"), col("RELATION_POS"), col("RELATION_ID"), col("RELATION_ROLE"), col("RELATION_TYPE"))
    val waysExp = waysExplode(ways).select(col("ID"), col("TYPE"), col("WAY_POS"), col("WAY_ID"))
    val waysFlt = waysExp.as("w").join(broadcast(relsExp).as("r"), col("w.ID") === col("r.RELATION_ID"), "inner")
      .select(
        col("r.ID"), col("r.TYPE"), col("r.RELATION_ROLE"), col("r.RELATION_POS"), col("r.RELATION_ID"), col("r.RELATION_TYPE"),
        col("w.ID").as("WAY_ID"), col("w.TYPE").as("WAY_TYPE"), col("WAY_POS").as("WAY_NODE_POS"), col("w.WAY_ID").as("WAY_NODE_ID")
      )
    val nodesFlt = nodes.select(col("ID"), col("TYPE"), col("LON"), col("LAT"))

    val result = nodesFlt.as("n")
      .join(broadcast(waysFlt).as("w"), col("n.ID") === col("w.WAY_NODE_ID"), "inner")
      .select(
        col("w.ID"), col("w.TYPE"), col("w.RELATION_ROLE"), col("w.RELATION_POS"), col("w.RELATION_ID"), col("w.RELATION_TYPE"),
        col("w.WAY_ID"), col("w.WAY_TYPE"), col("w.WAY_NODE_POS"), col("w.WAY_NODE_ID"),
        col("n.ID").as("NODE_ID"), col("n.TYPE").as("NODE_TYPE"), col("n.LON"), col("n.LAT")
      )

    result
  }
}
