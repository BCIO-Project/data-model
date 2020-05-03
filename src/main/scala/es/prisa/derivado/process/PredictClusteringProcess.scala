package es.prisa.derivado.process

import java.time.LocalDate
import java.util.Properties

import es.prisa.derivado.constants.ProcessIds.PredictClustering
import es.prisa.derivado.dao.ProcessDao
import es.prisa.derivado.util.IngestionType
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler

class PredictClusteringProcess(props: Properties, date: LocalDate, jobName:String, className:String) extends ProcessDao(props, date, jobName, className) {
  override val processName: IngestionType = PredictClustering
  val pConfig = readPredictClusteringConfig()

  override def executeProcess(props: Properties, date: LocalDate, jobType: String)(implicit spark: SparkSession): Unit = {

    val df = readParquetOrExit(pConfig.userVStagsInputPath)

    writeLog(s"#### Loading kmeansModel ####")
    //val kmeansModel = KMeansModel.load(pConfig.modelInputPath)
    val kmeansModel = readKMeansOrExit(pConfig.modelInputPath)

    val assembler = new VectorAssembler().
      setInputCols(df.columns.filter(_ != "userId")).
      setOutputCol("features")

    val dataset = assembler.transform(df)

    writeLog(s"#### Obtaining predictedDF ####")
    val predictedDF = kmeansModel.transform(dataset).select("userId", "features", "cluster")

    predictedDF.write.mode("overwrite").parquet(pConfig.predictedDataOutputPath)
  }
}

object PredictClusteringProcess {
  def apply(props: Properties, date: LocalDate, jobName:String, className:String): PredictClusteringProcess = new PredictClusteringProcess(props, date, jobName, className)
}