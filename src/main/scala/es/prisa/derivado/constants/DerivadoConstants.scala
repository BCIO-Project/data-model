package es.prisa.derivado.constants

import es.prisa.common.literals.BasicConstants

/**
  * Constants common for the ingestions
  */
object DerivadoConstants extends BasicConstants{
  val APPNAME: String = "BCIO" // normally is the name of the project module

  val ESCRITURAMODELOREDIS: String = "EscrituraModeloRedis"
  val GETINFOAPI: String = "GetInfoAPI"
  val MODELO30DAYS: String = "Modelo30DaysCampaignInfoCTR"
  val FORMATTINGDATA: String = "FormattingData"
  val TRAINCLUSTERING: String = "TrainClustering"
  val PREDICTCLUSTERING: String = "PredictClustering"
  val CONTENTSIMILARITY: String = "ContentSimilarity"
  val CALCULATECTR: String = "CalculateCTR"
  val CALCULATESCORE: String = "CalculateScore"

  val DERIVADO: String = "derivado.prisa."   // start key on properties entries
  val LAYERS:    String = ".layers"

  val BUCKETIN = DERIVADO   + APPNAME + LAYERS + sourceConstants.INPUT
  val BUCKETOUT= DERIVADO   + APPNAME + LAYERS + sourceConstants.OUTPUT

  val SOURCE_INPUT: String  =  DERIVADO  + sourceConstants.INPUT + "."
  val SOURCE_OUTPUT: String =  DERIVADO +  sourceConstants.OUTPUT + "."

  val COMPAT_DATE_FILE_FORMAT = "yyyy-MM-dd"
  val SLASH_DATE_FILE_FORMAT = "yyyy/MM/dd"

  val BASEPATH   =       ".basepath"

  val COALESCE    =      ".coalesce"
  val DEFAULT_COALESCE = 8

  val COLAESCECSV = 1
  val HEADER = ".header"
  val CHARSET = ".charset"
  val DELIMITER = ".delimiter"

  val BCIOBASEPATH = "BCIOBasePath"
  val APIPATH = ".API"
  val URL_API_TOKEN = ".url_api_token"
  val URL_API_CAMPAIGNS = ".url_api_campaigns"
  val HEADER_AUTH = ".header_auth"
  val TOKEN = ".bearer_token"

  val IP_REDIS = ".IP"
  val PUERTO_REDIS = ".Puerto"
  val JSON_PATH = ".json_bcio"
  val TTL = ".ttl"

  val EVENTSPATH = ".Events"
  val PROJECT_ID = ".project_id"
  val GS_BUCKET = ".gs_bucket"
  val DATASET = ".dataset"
  val TABLE = ".table"
  val LOCATION = ".location"
  val SELECT_LIST=".select_list"

  val COLAB = ".url_colab"
  val DICTBASEPATH = ".dictionaryBasePath"
  val OMNITUREPATH = ".omnitureBasePath"
  val CLICKERSPATH = ".BCIOBasePath_Clickers"
  val NAVINFOPATH  = ".BCIOBasePath_30days"
  val DAILYBASEPATH = ".dailyBasePath"
  val EVENTSJOINAPIPATH = ".BCIOBasePath_EventsAPI"
  val CAMPAIGNINFO30DAYSPATH = ".CampaignInfo30days"
  val NEWINFOPATH = ".newInfo"
  val CTRTAGSPATH = ".CTR_tags"
  val USERIDPATH = ".userId_tags"
  val USERVSTAGSPATH = ".userVStagsPath"
  val FORMATEDUSERIDTAGSPATH = ".formatedUserIdTagsPath"
  val OLDCONTENTSPATH = ".oldContentsPath"
  val NEWCONTENTSPATH = ".newContentsPath"
  val SIMILARITYPATH = ".similarityPath"

  val NUMCLUSTER = ".numCluster"
  val CLUSTERINGMODELPATH = ".clusteringModelPath"
  val CLUSTEREDDATAPATH = ".clusteredDataPath"
  val CTRBYCLUSTERPATH = ".ctrByClusterPath"
  val USERVSSCOREPATH= ".userVSscorePath"
}
