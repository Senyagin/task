import scala.io.Source
import java.io.PrintWriter
import java.io.File

case class VatReport(inn: String, period: String, nds: Int, ndscred: Int)
case class Invoice(date: String, sellerInn: String, buyerInn: String, nds: Int)
case class Expected(period: String, inn: String, ndsDiff: Int, ndscredDiff: Int)

object Main extends App {
  // Считываем данные из файлов
  val vatReportData = Source.fromFile("vat_report.csv").getLines().drop(1).map(line => {
    val data = line.split(",")
    VatReport(data(0), data(1), data(2).toInt, data(3).toInt)
  }).toList

  val invoiceData = Source.fromFile("invoices.csv").getLines().drop(1).map(line => {
    val data = line.split(",")
    Invoice(data(0), data(1), data(2), data(3).toInt)
  }).toList

  // Группируем данные по периоду и ИНН продавца в отчете НДС
  val vatReportGrouped = vatReportData.groupBy(record => (record.period, record.inn))

  // Считаем сумму НДС для каждого периода и ИНН продавца в отчете НДС
  val vatReportSum = vatReportGrouped.mapValues(records => records.map(_.nds).sum)

  // Группируем данные по периоду и ИНН продавца в счетах-фактурах
  val invoiceGroupedsel = invoiceData.groupBy(record => (record.date.split("-")(0) + record.date.split("-")(1), record.sellerInn))
  
  // Считаем сумму НДС для каждого периода и ИНН продавца в счетах-фактурах
  val invoiceSumsel = invoiceGroupedsel.mapValues(records => records.map(_.nds).sum)
  
  // Находим расхождения по НДС
  val ndsDiff = invoiceSumsel.map { case (key, value) =>
    val reportVat = vatReportSum.getOrElse(key, 0)
    val diff = math.abs(value - reportVat)
    if (diff > 0) {
      Some(Expected(key._1, key._2, diff, 0))
    } else {
      None
    }
  }.flatten

  // Считаем сумму НДС к зачету для каждого периода и ИНН продавца в отчете НДС
  val vatReportSumcred = vatReportGrouped.mapValues(records => records.map(_.ndscred).sum)

  // Группируем данные по периоду и ИНН покупателя в счетах-фактурах
  val invoiceGroupedbuy = invoiceData.groupBy(record => (record.date.split("-")(0) + record.date.split("-")(1), record.buyerInn))

  // Считаем сумму НДС для каждого периода и ИНН продавца в счетах-фактурах
  val invoiceSumbuy = invoiceGroupedbuy.mapValues(records => records.map(_.nds).sum)

  // Находим расхождения по зачету
  val ndscredDiff = vatReportSumcred.map { case (key, value) =>
    val reportVat = invoiceSumbuy.getOrElse(key, 0)
    if (reportVat < value) {
      val diff = math.abs(reportVat - value)
      Some(Expected(key._1, key._2, 0, diff))
    } else {
      None
    }
  }.flatten
  
  // Объединяем расхождения по НДС и по зачету, сортируем по дате
  val discrepancies = (ndsDiff ++ ndscredDiff).toList.sortBy(_.period)

  // Записываем результаты в файл "expected.csv"
  val writer = new PrintWriter(new File("expected.csv"))
  writer.println("Дата счета-фактуры,ИНН продавца,расхождения сумм НДС,сумма НДС взятых к зачету")
  discrepancies.foreach { case Expected(period, inn, ndsDiff, ndscredDiff) =>
    writer.println(s"$period,$inn,$ndsDiff,$ndscredDiff")
  }
  writer.close()
}
