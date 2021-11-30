package utils

import scala.util.matching.Regex

trait CommandLineParser {
  val pairOpt: Regex = "--([^=]+)=([^=]+)".r

  def getOpts(args: Array[String]): Map[String,String]= {
    val (opts, _) = args.partition(_.startsWith("--"))
    opts.collect{
      case pairOpt(key, value) => key -> value
    }.toMap
  }
}
