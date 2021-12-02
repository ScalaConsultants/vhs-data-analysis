package config

sealed trait ReaderConfig

final case class MongoReaderConfig(mongoUri: String, database: String, collection: String) extends ReaderConfig

final case class LocalFileReaderConfig(mainPath: String, folderName: String) extends ReaderConfig

object ReaderConfig {

  private def getMongoReaderConfigFromOpts(mapOpts: Map[String, String]): Option[MongoReaderConfig] = {
    val mongoReaderConfigOpts = (mapOpts.get("mongoUri"), mapOpts.get("database"), mapOpts.get("collection"))
    mongoReaderConfigOpts match {
      case (Some(mongoUri), Some(database), Some(collection)) => Some(MongoReaderConfig(mongoUri, database, collection))
      case _ => None
    }
  }

  private def getLocalFileReaderConfigFromOpts(mapOpts: Map[String, String]): Option[LocalFileReaderConfig] = {
    val localFileReaderConfigOpts = (mapOpts.get("mainPath"), mapOpts.get("folderName"))
    localFileReaderConfigOpts match {
      case (Some(mainPath), Some(folderName)) => Some(LocalFileReaderConfig(mainPath, folderName))
      case _ => None
    }
  }

  def getReaderConfigFromOpts(mapOpts: Map[String, String]): Either[String, ReaderConfig] = {
    val mongoReaderConfig = getMongoReaderConfigFromOpts(mapOpts)
    val localFileReaderConfig = getLocalFileReaderConfigFromOpts(mapOpts)

    (mongoReaderConfig, localFileReaderConfig) match {
      case (Some(mongoReaderConf), None) => Right(mongoReaderConf)
      case (None, Some(localFileReaderConf)) => Right(localFileReaderConf)
      case (Some(_), Some(_)) => Left("ambiguous readerConfig")
      case (None, None) => Left("readerConfig was not provided")
    }
  }
}