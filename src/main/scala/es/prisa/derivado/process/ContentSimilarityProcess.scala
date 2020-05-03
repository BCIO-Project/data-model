package es.prisa.derivado.process

import java.time.LocalDate
import java.util.Properties

import es.prisa.derivado.constants.ProcessIds.ContentSimilarity
import es.prisa.derivado.dao.ProcessDao
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.distributed.{MatrixEntry, RowMatrix}

class ContentSimilarityProcess (props: Properties, date: LocalDate, jobName:String, className:String) extends ProcessDao(props, date, jobName, className) {
  override val processName = ContentSimilarity
  val pConfig = readContentSimilarityConfig()

  override def executeProcess(props: Properties, date: LocalDate, sourceName : String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    // Contenidos de los últimos 30 días
    val oldContents = readParquetOrExit(pConfig.oldContentsInputPath).
      withColumn("contentId", concat($"pageId", lit("_"), $"campaignId", lit("_"), $"offerId")).
      withColumn("type", lit("old")).
      filter(!'tag.isNull).
      select("contentId", "tag", "type")

    // Contenidos nuevos
    val newContents = readParquetOrExit(pConfig.newContentsInputPath).
      withColumn("contentId", concat($"pageId", lit("_"), $"campaignId", lit("_"), $"offerId")).
      withColumn("type", lit("new")).
      filter(!'tag.isNull).
      select("contentId", "tag", "type")

    // Union de todos los contenidos y estandarización del formato de las tags
    val contents = oldContents.union(newContents).
      withColumn("standardTag", standarizeTagName( 'tag)).
      select("contentId", "standardTag", "type").
      withColumnRenamed("standardTag", "tag")

    // Creando identificador numérico para cada contenido

    val contentIds = contents.select('contentId).distinct

    val rddContentIds = contentIds.rdd.map( r => r.getString(0)).zipWithIndex()
    val contentIndex = rddContentIds.toDF( "contentId", "id")

    writeLog(s"#### Obtaining contentsWithNumId ####")
    val contentsWithNumId = contents.
      join(contentIndex, "contentId").
      withColumn("value", lit(1)).
      select( 'id, 'contentId, 'tag, 'value, 'type)

    // Convirtiendo de DataFrame a RDD con dense vectors

    //val tagsList = contentsWithNumId.select( 'tag).distinct.map( _.getString(0)).collect.sorted
    val tagsList = contentsWithNumId.select( 'tag).distinct.map( _.getString(0)).collect.sorted
    val broadcastTagsSeq = spark.sparkContext.broadcast( tagsList)

    val contentsRdd = contentsWithNumId.rdd.map{
      row =>
        (row.getLong(0)) -> (row.getString(2), row.getInt(3))
    }.aggregateByKey(scala.collection.mutable.Map[String,Long]())({
      case (m, (tag, count)) => m + (tag -> count)
    }, _ ++ _).mapValues{
      m =>
        val seqTags = m.toSeq.sortBy(_._1)
        Vectors.sparse(broadcastTagsSeq.value.size,
          seqTags.map(_._1).map(broadcastTagsSeq.value.indexOf(_)).toArray,
          seqTags.map(_._2.toDouble).toArray)
    }.map{
      case( id, tags_sparse) => ( id, tags_sparse, tags_sparse.toDense)
    }.sortBy(_._1)

    writeLog(s"#### Obtaining bcVStagMatrix ####")
    val bcVStagMatrix= new RowMatrix( contentsRdd.coalesce(1).
      map{case( contentId, tags_sparse, tags_sparse_toDense) => ( tags_sparse_toDense)})

    writeLog(s"#### Obtaining tagVSbcMatrix ####")
    val tagVSbcMatrix = transposeRowMatrix( bcVStagMatrix)

    val cosineSimilarity = tagVSbcMatrix.columnSimilarities()

    // Vinculando el valor de similitud calculado con cada par de contentIds
    writeLog(s"#### Obtaining similarities ####")

    val similarities = cosineSimilarity.
      entries.map{ case MatrixEntry( i, j, u) => ( i, j, u)}.
      toDF( "contentId1", "contentId2", "sim").
      persist

    val similaritiesDF = similarities.
      join( contentIndex, similarities.col( "contentId1") === contentIndex.col( "id"), "left").
      select( 'contentId as 'contentId1, 'contentId2, 'sim).
      join( contentIndex, similarities.col( "contentId2") === contentIndex.col( "id"), "left").
      select( 'contentId1, 'contentId as 'contentId2, 'sim).
      persist

    // Filtrado por las similitudes de contenidos "old" con "new" que son las que necesitamos

    val similaritiesWithTypeDF = similaritiesDF.
      join( contents.dropDuplicates("contentId", "type"),
        similaritiesDF.col( "contentId1") === contents.col( "contentId"), "left").
      select( 'contentId1, 'contentId2, 'sim, 'type as 'type_contentId1).
      join( contents.dropDuplicates("contentId", "type"),
        similaritiesDF.col( "contentId2") === contents.col( "contentId"), "left").
      select( 'contentId1, 'contentId2, 'sim, 'type_contentId1, 'type as 'type_contentId2).
      persist

    val filteredSimilaritiesDF = similaritiesWithTypeDF.
      filter(('type_contentId1 === "old" && 'type_contentId2 === "new") ||
        ('type_contentId1 === "new" && 'type_contentId2 === "old")).
      withColumn( "newContent", when('type_contentId1 === "new", 'contentId1).otherwise('contentId2)).
      withColumn( "oldContent", when('type_contentId1 === "old", 'contentId1).otherwise('contentId2)).
      select("newContent", "oldContent", "sim")

    // Incluyendo todas las parejas de newContent y oldContent
    val allpair = newContents.
      select("contentId").distinct.
      withColumnRenamed("contentId", "newContent").
      crossJoin(
        oldContents.
          select("contentId").distinct.
          withColumnRenamed("contentId", "oldContent")
      )
    
    val allSimilarities = allpair.join(filteredSimilaritiesDF, Seq("newContent", "oldContent"), "left").
      withColumn("allsim", when(! $"sim".isNull, $"sim").
                           when($"sim".isNull and $"newContent" === $"oldContent", 1).
                           otherwise(0)).
      select("newContent", "oldContent", "allsim").
      withColumnRenamed("allsim", "sim")	
	
    // Guardar resultados
    writeData(allSimilarities, pConfig.similarityOutputPath)
  }

  // Para estandarizar los nombres de las tags
  val standarizeTagName = udf( ( a:String) =>  a.toLowerCase.replaceAll("á", "a").
    replaceAll("é", "e").
    replaceAll("í", "i").
    replaceAll("ó", "o").
    replaceAll("ú", "u").
    replaceAll(" ","_"))

  // Transformación de una fila para trasponer una matriz
  def rowToTransposedTriplet(row: Vector, rowIndex: Long): Array[(Long, (Long, Double))] = {
    val indexedRow = row.toArray.zipWithIndex
    indexedRow.map{case (value, colIndex) => (colIndex.toLong, (rowIndex, value))}
  }

  // Transformación de una fila para trasponer una matriz
  def buildRow(rowWithIndexes: Iterable[(Long, Double)]): Vector = {
    val resArr = new Array[Double](rowWithIndexes.size)
    rowWithIndexes.foreach{case (index, value) =>
      resArr(index.toInt) = value
    }
    Vectors.dense(resArr)
  }

  // Traspone una matriz
  def transposeRowMatrix(m: RowMatrix): RowMatrix = {

    val transposedRowsRDD = m.
      rows.
      zipWithIndex.
      map{case (row, rowIndex) => rowToTransposedTriplet(row, rowIndex)}.
      flatMap(x => x).
      groupByKey.
      sortByKey().
      map(_._2).
      map(buildRow)

    new RowMatrix(transposedRowsRDD)
  }
}

object ContentSimilarityProcess {
  def apply(props: Properties, date: LocalDate, jobName:String, className:String): ContentSimilarityProcess = new ContentSimilarityProcess(props, date, jobName, className)
}