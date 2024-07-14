import com.wolt.osm.spark.OsmSource.OsmSource
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import util.{OsmReader, SparkConnection}

object Main extends SparkConnection {
  def main(args: Array[String]): Unit = {

    // path to load file russia-latest.osm.pbf from https://download.geofabrik.de/russia-latest.osm.pbf
    val sourcePath = args.head

    // path save result files *.parquet
    val targetPath = args.tail.head

    println(s"Path to load *.pbf file: $sourcePath")

    val spark = create(name = "OSM Read *.pbf file")

    val osm: DataFrame = OsmReader.osm(sourcePath, spark)

    println(s"OSM schema:\n${osm.schema.treeString}")
    /**OSM schema:
     * root
     * |-- ID: long (nullable = false)
     * |-- TAG: map (nullable = false)
     * |    |-- key: string
     * |    |-- value: string (valueContainsNull = false)
     * |-- INFO: struct (nullable = true)
     * |    |-- UID: integer (nullable = true)
     * |    |-- USERNAME: string (nullable = true)
     * |    |-- VERSION: integer (nullable = true)
     * |    |-- TIMESTAMP: long (nullable = true)
     * |    |-- CHANGESET: long (nullable = true)
     * |    |-- VISIBLE: boolean (nullable = false)
     * |-- TYPE: integer (nullable = false)
     * |-- LAT: double (nullable = true)
     * |-- LON: double (nullable = true)
     * |-- WAY: array (nullable = true)
     * |    |-- element: long (containsNull = false)
     * |-- RELATION: array (nullable = true)
     * |    |-- element: struct (containsNull = false)
     * |    |    |-- ID: long (nullable = false)
     * |    |    |-- ROLE: string (nullable = true)
     * |    |    |-- TYPE: integer (nullable = false)
     */

    val nodes     = OsmReader.nodes(osm)
    val ways      = OsmReader.ways(osm)
    val relations = OsmReader.relations(osm)

    println(s"Partitions: ${osm.rdd.partitions.length} OSM: ${osm.count} Nodes: ${nodes.count}, Ways: ${ways.count}, Relations: ${relations.count}")

    println(s"Path to save results: $targetPath")

    write(nodes    , targetPath + "\\nodes")
    write(ways     , targetPath + "\\ways")
    write(relations, targetPath + "\\relations")
  }

  def write(df: DataFrame, path: String): Unit = {
    df.write.format("parquet").mode(SaveMode.Overwrite).save(path)
  }
}
