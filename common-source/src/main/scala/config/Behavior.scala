package config

sealed trait Behavior {
  def label: String
}

case object Daily extends Behavior {
  def label: String = "daily"
}
case object Monthly extends Behavior {
  def label: String = "monthly"
}
case object Both extends Behavior {
  def label: String = "both"
}
