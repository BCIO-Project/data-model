package es.prisa.derivado.dao

import java.io.FileNotFoundException
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Properties

import es.prisa.common.arq.process.BasicProcess
import es.prisa.derivado.constants.DerivadoConstants._
import es.prisa.derivado.util.IngestionType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.clustering.KMeansModel


/**
  * Funciones implicitas para los procesos
  *
  * @author INDRA
  */

abstract class ProcessDao(props:Properties, date:LocalDate, jobName:String, className:String) extends BasicProcess(jobName,className) {
  val processName: IngestionType

  /**
    * Compose input and output path based on configuration params
    *
    * @return Config (variables)
    */

  case class GetInfoAPIConfig(BCIOBasePath: String, url_api_token: String, header_auth:String, url_api_campaigns:String, fecha_cts:String, execDate:String, output_path:String)

  def readGetInfoAPIConfig(): GetInfoAPIConfig = {

    val datetime = date.atTime(0,0, 0)
    val dateTimestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val fecha_cts = dateTimestamp.format(datetime)
    val dateFormatterRead = DateTimeFormatter.ofPattern("yyyy/MM/dd")
    val execDate = dateFormatterRead.format(datetime)

    val BCIOBasePath = props.getProperty(BCIOBASEPATH)
    val BCIOBasePath_API = BCIOBasePath + props.getProperty(GETINFOAPI + APIPATH)
    val url_api_token = props.getProperty(GETINFOAPI + URL_API_TOKEN)
    val url_api_campaigns = props.getProperty(GETINFOAPI + URL_API_CAMPAIGNS)
    val header_auth = props.getProperty(GETINFOAPI + HEADER_AUTH)

    val output_path = BCIOBasePath_API + execDate

    writeLog("+-----------------------------------------------------------------------------------+")
    writeLog(s"  header_auth:  $header_auth")
    writeLog(s"  url_api_campaigns:  $url_api_campaigns")
    writeLog(s"  fecha_cts:  $fecha_cts")
    writeLog(s"  execDate:  $execDate")
    writeLog(s"  output_path:  $output_path")
    writeLog("+-----------------------------------------------------------------------------------+")


    val myConf = GetInfoAPIConfig(BCIOBasePath, url_api_token, header_auth, url_api_campaigns, fecha_cts, execDate, output_path)
    myConf
  }

  case class EscrituraModeloRedisConfig(IP: String, Puerto: Int, json_path: String, userVSscoreInputPath: String, ttl: Int)

  def readEscrituraModeloRedisConfig(): EscrituraModeloRedisConfig = {

    //Otros
    val IP = props.getProperty(ESCRITURAMODELOREDIS + IP_REDIS)
    val Puerto = props.getProperty(ESCRITURAMODELOREDIS + PUERTO_REDIS).toInt
    val json_path = props.getProperty(ESCRITURAMODELOREDIS + JSON_PATH)
    val ttl = props.getProperty(ESCRITURAMODELOREDIS + TTL).toInt

    //Fechas
    val dateFormat = DateTimeFormatter.ofPattern("yyyy/MM/dd")
    val execDate = date
    val execDate_formatted= execDate.format(dateFormat)

    //GS paths
    val BCIOBasePath = props.getProperty(BCIOBASEPATH)
    val userVSscorePath = BCIOBasePath + props.getProperty(ESCRITURAMODELOREDIS + USERVSSCOREPATH)

    val userVSscoreInputPath = userVSscorePath + execDate_formatted


    writeLog("+-----------------------------------------------------------------------------------+")
    writeLog(s"  IP:  $IP")
    writeLog(s"  Puerto:  $Puerto")
    writeLog(s"  JSON:  $json_path")
    writeLog(s"  userVSscoreInputPath: $userVSscoreInputPath")
    writeLog(s"  ttl: $ttl")
    writeLog("+-----------------------------------------------------------------------------------+")

    val myConf = EscrituraModeloRedisConfig(IP, Puerto, json_path, userVSscoreInputPath, ttl)
    myConf
  }

  case class Modelo30DaysCampaignInfoCTRConfig(project_id:String, dataset:String, table:String, bqDatasetLocation:String,
                                 gs_bucket:String, inputPaths30Days:List[String], newCampaignInfoInputPath:String,
                                               campaignInfo30DaysOutputPath:String, newInfoOutputPath:String,
                                               eventsCTROutputPath:String, eventsUserIdOutputPath:String)

  def readModelo30DaysCampaignInfoCTRConfig(): Modelo30DaysCampaignInfoCTRConfig = {
    //Fechas
    val dateFormat = DateTimeFormatter.ofPattern("yyyy/MM/dd")
    val execDate = date
    val execDate_formatted= execDate.format(dateFormat)

    val initialDate = execDate.minusDays(31)
    val endDate = execDate.minusDays(1)
    var dates_list  = List[String]()
    var auxDate = initialDate

    while (auxDate.toEpochDay() <= endDate.toEpochDay()){
      dates_list = auxDate.format(dateFormat) :: dates_list
      auxDate = auxDate.plusDays(1)
    }

    //BigQuery
    val project_id = props.getProperty(MODELO30DAYS + PROJECT_ID)
    val dataset = props.getProperty(MODELO30DAYS + DATASET)
    val table = props.getProperty(MODELO30DAYS + TABLE)
    val bqDatasetLocation = props.getProperty(MODELO30DAYS + LOCATION)
    val gs_bucket = props.getProperty(MODELO30DAYS + GS_BUCKET)

    //GS paths
    val BCIOBasePath = props.getProperty(BCIOBASEPATH)
    val BCIOBasePath_API = BCIOBasePath + props.getProperty(MODELO30DAYS + APIPATH)
    val BCIOBasePath_30days = BCIOBasePath + props.getProperty(MODELO30DAYS + CAMPAIGNINFO30DAYSPATH)
    val BCIOBasePath_newInfo = BCIOBasePath + props.getProperty(MODELO30DAYS + NEWINFOPATH)
    val BCIOBasePath_CTR_tags = BCIOBasePath + props.getProperty(MODELO30DAYS + CTRTAGSPATH)
    val BCIOBasePath_userId_tags = BCIOBasePath + props.getProperty(MODELO30DAYS + USERIDPATH)

    val inputPaths30Days = dates_list.map(x => BCIOBasePath_API + x)
    val newCampaignInfoInputPath = BCIOBasePath_API + execDate_formatted
    val campaignInfo30DaysOutputPath = BCIOBasePath_30days + execDate_formatted
    val newInfoOutputPath = BCIOBasePath_newInfo + execDate_formatted
    val eventsCTROutputPath = BCIOBasePath_CTR_tags + execDate_formatted
    val eventsUserIdOutputPath = BCIOBasePath_userId_tags + execDate_formatted

    writeLog("+-----------------------------------------------------------------------------------+")
    writeLog(s"  project_id:  $project_id")
    writeLog(s"  dataset:  $dataset")
    writeLog(s"  table:  $table")
    writeLog(s"  bqDatasetLocation:  $bqDatasetLocation")
    writeLog(s"  gs_bucket:  $gs_bucket")
    writeLog(s"  inputPaths30Days:  $inputPaths30Days")
    writeLog(s"  newCampaignInfoInputPath:  $newCampaignInfoInputPath")
    writeLog(s"  campaignInfo30DaysOutputPath:  $campaignInfo30DaysOutputPath")
    writeLog(s"  newInfoOutputPath:  $newInfoOutputPath")
    writeLog(s"  eventsCTROutputPath:  $eventsCTROutputPath")
    writeLog(s"  eventsUserIdOutputPath:  $eventsUserIdOutputPath")
    writeLog("+-----------------------------------------------------------------------------------+")

    val myConf = Modelo30DaysCampaignInfoCTRConfig(project_id, dataset, table, bqDatasetLocation, gs_bucket,
      inputPaths30Days, newCampaignInfoInputPath, campaignInfo30DaysOutputPath, newInfoOutputPath,
      eventsCTROutputPath, eventsUserIdOutputPath)
    myConf
  }

  case class FormattingDataConfig(userVStagsInputPath:String, formattedOutputPath:String)

  def readFormattingDataConfig(): FormattingDataConfig = {
    //Fechas
    val dateFormat = DateTimeFormatter.ofPattern("yyyy/MM/dd")
    val execDate = date
    val execDate_formatted= execDate.format(dateFormat)

    //GS paths
    val BCIOBasePath = props.getProperty(BCIOBASEPATH)
    val userVStagsPath = BCIOBasePath + props.getProperty(FORMATTINGDATA + USERVSTAGSPATH)
    val formattedPath = BCIOBasePath + props.getProperty(FORMATTINGDATA + FORMATEDUSERIDTAGSPATH)

    val userVStagsInputPath = userVStagsPath + execDate_formatted
    val formattedOutputPath = formattedPath + execDate_formatted

    writeLog("+-----------------------------------------------------------------------------------+")
    writeLog(s"  inputPath:  $userVStagsInputPath")
    writeLog(s"  outputPath:  $formattedOutputPath")
    writeLog("+-----------------------------------------------------------------------------------+")

    val myConf = FormattingDataConfig(userVStagsInputPath, formattedOutputPath)
    myConf
  }

  case class TrainClusteringConfig(userVStagsInputPath:String, modelPathOutputPath:String, numCluster:Int)

  def readTrainClusteringConfig(): TrainClusteringConfig = {
    //Fechas
    val dateFormat = DateTimeFormatter.ofPattern("yyyy/MM/dd")
    val execDate = date
    val execDate_formatted= execDate.format(dateFormat)

    //GS paths
    val BCIOBasePath = props.getProperty(BCIOBASEPATH)
    val userVStagsPath = BCIOBasePath + props.getProperty(TRAINCLUSTERING + USERVSTAGSPATH)
    val modelPathOutputPath = BCIOBasePath + props.getProperty(TRAINCLUSTERING + CLUSTERINGMODELPATH)

    val userVStagsInputPath = userVStagsPath + execDate_formatted

    //Otros
    val numCluster = props.getProperty(TRAINCLUSTERING + NUMCLUSTER).toInt

    writeLog("+-----------------------------------------------------------------------------------+")
    writeLog(s"  userVStagsInputPath:  $userVStagsInputPath")
    writeLog(s"  modelPathOutputPath:  $modelPathOutputPath")
    writeLog(s"  numCluster:  $numCluster")
    writeLog("+-----------------------------------------------------------------------------------+")

    val myConf = TrainClusteringConfig(userVStagsInputPath, modelPathOutputPath, numCluster)
    myConf
  }

  case class PredictClusteringConfig(modelInputPath:String, userVStagsInputPath:String, predictedDataOutputPath:String)

  def readPredictClusteringConfig(): PredictClusteringConfig = {
    //Fechas
    val dateFormat = DateTimeFormatter.ofPattern("yyyy/MM/dd")
    val execDate = date
    val execDate_formatted= execDate.format(dateFormat)

    //GS paths
    val BCIOBasePath = props.getProperty(BCIOBASEPATH)
    val userVStagsPath = BCIOBasePath + props.getProperty(PREDICTCLUSTERING + USERVSTAGSPATH)
    val predictedDataPath = BCIOBasePath + props.getProperty(PREDICTCLUSTERING + CLUSTEREDDATAPATH)

    val modelInputPath = BCIOBasePath + props.getProperty(PREDICTCLUSTERING + CLUSTERINGMODELPATH)
    val userVStagsInputPath = userVStagsPath + execDate_formatted
    val predictedDataOutputPath = predictedDataPath + execDate_formatted

    writeLog("+-----------------------------------------------------------------------------------+")
    writeLog(s"  modelInputPath:  $modelInputPath")
    writeLog(s"  userVStagsInputPath:  $userVStagsInputPath")
    writeLog(s"  predictedDataOutputPath:  $predictedDataOutputPath")
    writeLog("+-----------------------------------------------------------------------------------+")

    val myConf = PredictClusteringConfig(modelInputPath, userVStagsInputPath, predictedDataOutputPath)
    myConf
  }

  case class ContentSimilarityConfig(oldContentsInputPath:String, newContentsInputPath:String, similarityOutputPath:String)

  def readContentSimilarityConfig(): ContentSimilarityConfig = {
    //Fechas
    val dateFormat = DateTimeFormatter.ofPattern("yyyy/MM/dd")
    val execDate = date
    val execDate_formatted= execDate.format(dateFormat)

    //GS paths
    val BCIOBasePath = props.getProperty(BCIOBASEPATH)
    val oldContentsPath = BCIOBasePath + props.getProperty(CONTENTSIMILARITY + OLDCONTENTSPATH)
    val newContentsPath = BCIOBasePath + props.getProperty(CONTENTSIMILARITY + NEWCONTENTSPATH)
    val similarityPath = BCIOBasePath + props.getProperty(CONTENTSIMILARITY + SIMILARITYPATH)

    val oldContentsInputPath = oldContentsPath + execDate_formatted
    val newContentsInputPath = newContentsPath + execDate_formatted
    val similarityOutputPath = similarityPath + execDate_formatted

    writeLog("+-----------------------------------------------------------------------------------+")
    writeLog(s"  oldContentsInputPath:  $oldContentsInputPath")
    writeLog(s"  newContentsInputPath:  $newContentsInputPath")
    writeLog(s"  similarityOutputPath:  $similarityOutputPath")
    writeLog("+-----------------------------------------------------------------------------------+")

    val myConf = ContentSimilarityConfig(oldContentsInputPath, newContentsInputPath, similarityOutputPath)
    myConf
  }

  case class CalculateCTRConfig(ctrInputPath:String, clusteredDataInputPath:String, ctrByClusterOutputPath:String)

  def readCalculateCTRConfig(): CalculateCTRConfig = {
    //Fechas
    val dateFormat = DateTimeFormatter.ofPattern("yyyy/MM/dd")
    val execDate = date
    val execDate_formatted= execDate.format(dateFormat)

    //GS paths
    val BCIOBasePath = props.getProperty(BCIOBASEPATH)
    val ctrPath = BCIOBasePath + props.getProperty(CALCULATECTR + CTRTAGSPATH)
    val clusteredDataPath = BCIOBasePath + props.getProperty(CALCULATECTR + CLUSTEREDDATAPATH)
    val ctrByClusterPath = BCIOBasePath + props.getProperty(CALCULATECTR + CTRBYCLUSTERPATH)

    val ctrInputPath = ctrPath + execDate_formatted
    val clusteredDataInputPath = clusteredDataPath + execDate_formatted
    val ctrByClusterOutputPath = ctrByClusterPath + execDate_formatted

    writeLog("+-----------------------------------------------------------------------------------+")
    writeLog(s"  ctrInputPath:  $ctrInputPath")
    writeLog(s"  clusteredDataInputPath:  $clusteredDataInputPath")
    writeLog(s"  ctrByClusterOutputPath:  $ctrByClusterOutputPath")
    writeLog("+-----------------------------------------------------------------------------------+")

    val myConf = CalculateCTRConfig(ctrInputPath, clusteredDataInputPath, ctrByClusterOutputPath)
    myConf
  }

  case class CalculateScoreConfig(modelInputPath:String, clusteredDataInputPath:String, ctrByClusterInputPath:String, similarityInputPath:String, userVSscoreOutputPath:String)

  def readCalculateScoreConfig(): CalculateScoreConfig = {
    //Fechas
    val dateFormat = DateTimeFormatter.ofPattern("yyyy/MM/dd")
    val execDate = date
    val execDate_formatted= execDate.format(dateFormat)

    //GS paths
    val BCIOBasePath = props.getProperty(BCIOBASEPATH)
    val clusteredDataPath = BCIOBasePath + props.getProperty(CALCULATESCORE + CLUSTEREDDATAPATH)
    val ctrByClusterPath = BCIOBasePath + props.getProperty(CALCULATESCORE + CTRBYCLUSTERPATH)
    val similarityPath = BCIOBasePath + props.getProperty(CALCULATESCORE + SIMILARITYPATH)
    val userVSscorePath = BCIOBasePath + props.getProperty(CALCULATESCORE + USERVSSCOREPATH)

    val modelInputPath = BCIOBasePath + props.getProperty(CALCULATESCORE + CLUSTERINGMODELPATH)
    val clusteredDataInputPath = clusteredDataPath + execDate_formatted
    val ctrByClusterInputPath = ctrByClusterPath + execDate_formatted
    val similarityInputPath = similarityPath + execDate_formatted
    val userVSscoreOutputPath = userVSscorePath + execDate_formatted

    writeLog("+-----------------------------------------------------------------------------------+")
    writeLog(s"  modelInputPath:  $modelInputPath")
    writeLog(s"  clusteredDataInputPath:  $clusteredDataInputPath")
    writeLog(s"  ctrByClusterInputPath:  $ctrByClusterInputPath")
    writeLog(s"  similarityInputPath:  $similarityInputPath")
    writeLog(s"  userVSscoreOutputPath:  $userVSscoreOutputPath")
    writeLog("+-----------------------------------------------------------------------------------+")

    val myConf = CalculateScoreConfig(modelInputPath, clusteredDataInputPath, ctrByClusterInputPath, similarityInputPath, userVSscoreOutputPath)
    myConf
  }

  /**
    * Function read file parquet
    *
    * @param spark
    * @return
    */
  def readParquet(inputPath: String)(implicit spark: SparkSession): DataFrame = {
    writeLog(s"  readParquet:    $inputPath")
    val readParquet = spark.read.parquet(inputPath).toDF()

    readParquet
  }

  def readParquetOrExit(inputPath: String)(implicit spark: SparkSession): DataFrame = {
    writeLog(s"  readParquet:    $inputPath")
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    val path = new Path(inputPath)
    val fs = path.getFileSystem(conf)

    if(!fs.exists(path)){
      writeLog(s"Path does not exist: $path")
      System.exit(1)
    }

    val df = spark.read.parquet(inputPath)

    if (df.rdd.isEmpty()){
      writeLog(s"File is empty: $path")
      System.exit(1)
    }

    df
  }

  def readKMeansOrExit(inputPath: String)(implicit spark: SparkSession): KMeansModel = {
    writeLog(s"  readParquet:    $inputPath")
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    val path = new Path(inputPath)
    val fs = path.getFileSystem(conf)

    if (!fs.exists(path)) {
      writeLog(s"Path does not exist: $path")
      System.exit(1)
    }

    val kmeans = KMeansModel.load(inputPath)
    kmeans
  }

  def writeData(dataFrame: DataFrame, outputPath:String)(implicit spark: SparkSession): Unit = {
    writeLog( s"Writing $dataFrame on $outputPath")

    dataFrame.write.mode(SaveMode.Overwrite).parquet(outputPath)
  }

  def readMultipleParquetFiles(paths: Seq[String])(implicit spark: SparkSession): DataFrame = {
    paths.map(spark.read.parquet(_)).reduce(_.union(_))
  }

  // Función para concatenar todos los DFs con la información de navegación.
  def concatInfo(dfs : Seq[DataFrame]) : DataFrame = {
    dfs.reduce(_.union(_))
  }
}
