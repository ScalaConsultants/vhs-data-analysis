package config

sealed trait ReaderConfig

final case class MongoReaderConfig(mongoUri: String, database: String, collection: String) extends ReaderConfig

final case class AppConfig(readerConfig: ReaderConfig)

object AppConfig {
  def load(args: Array[String]): Option[AppConfig] = args match {
      case Array(mongoUri, database, collection) =>
        Some(AppConfig(MongoReaderConfig(mongoUri, database, collection)))
      case _ => None
    }
}