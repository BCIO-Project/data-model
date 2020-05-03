package es.prisa.derivado.process

import java.time.LocalDate
import java.util.Properties

import es.prisa.derivado.constants.ProcessIds.TrainClustering
import es.prisa.derivado.dao.ProcessDao
import es.prisa.derivado.util.IngestionType
import org.apache.spark.sql.SparkSession

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.clustering.KMeans

class TrainClusteringProcess(props: Properties, date: LocalDate, jobName:String, className:String) extends ProcessDao(props, date, jobName, className) {
  override val processName: IngestionType = TrainClustering
  val pConfig = readTrainClusteringConfig()

  override def executeProcess(props: Properties, date: LocalDate, jobType: String)(implicit spark: SparkSession): Unit = {

    var numCluster = pConfig.numCluster.toInt

    val formatedData = readParquetOrExit(pConfig.userVStagsInputPath)
    val numUsers = formatedData.count

    if(numCluster < 2 && numUsers > 5) {
        // Número de clusters por defecto numCluster = sqrt(numUserss/2)
        numCluster = Math.round(Math.sqrt(numUsers / 2)).toInt
    }else {
        if(numCluster < 2 || numCluster >= numUsers || numUsers < 3){
            writeLog("Error: The number of users: "+numUsers+" or the number of clusters: "+numCluster+" are not as expected. See the help.")
            System.exit(1)
        }
    }

    var errorsByCluster = Array[(Int, Int, Float)]()
    val numIterations = 10000

    val assembler = new VectorAssembler().
      setInputCols(formatedData.columns.filter(_ != "userId")).
      setOutputCol("features")

    val kmeans = new KMeans().
      setK(numCluster).
      setMaxIter(numIterations).
      setPredictionCol("cluster").
      setFeaturesCol("features").
      setTol(1.0E-2).
      setSeed(1L)

    val dataset = assembler.transform(formatedData)

    writeLog(s"#### Obtaining kmeansModel ####")
    val kmeansModel = kmeans.fit(dataset)

    kmeansModel.write.overwrite().save(pConfig.modelPathOutputPath)
  }
}

object TrainClusteringProcess {
  def apply(props: Properties, date: LocalDate, jobName:String, className:String): TrainClusteringProcess = new TrainClusteringProcess(props, date, jobName, className)
}