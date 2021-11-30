package config

sealed trait ReaderConfig

final case class MongoReaderConfig(mongoUri: String, database: String, collection: String) extends ReaderConfig

final case class LocalFileReaderConfig(mainPath: String, folderName: String) extends ReaderConfig