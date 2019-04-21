package data_analysis_on_sample_data.domain


case class Company(
                    id: String,
                    tradeStyleNames: Array[TradeStyleNames],
                    telephone: Array[Telephone],
                    primaryAddress: PrimaryAddress
                  ) {}

case class TradeStyleNames(name: String, priority: Int) {}

case class Telephone(telephoneNumber: String, isdCode: String, isUnreachable: Boolean) {}

case class PrimaryAddress(line1: String, line2: String) {}