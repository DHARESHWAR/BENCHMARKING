package parquet.speedtest

import java.math.MathContext
import java.sql.{Date, Timestamp}
import java.util

import org.apache.commons.lang3.{RandomStringUtils, RandomUtils}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object GenerateDataset {

  val TEAM_LIST: List[String] = List("DATA", "REPORTING", "REVENUE", "FINANCE", "PORTAL", "BI")
  val PROJECT_LIST: List[String] = List("DASHBOARD", "DATA-ANALYTICS", "PLATFORM")

  def getDataset(count: Int, spark: SparkSession): Dataset[Row] = {
    var employeeDetails = new util.ArrayList[Employee]()
    for (i <- 1 to count) {
      var employee = new Employee
      employee.id = i
      employee.uniqueId = generateLongOfN(10)
      employee.firstName = generateStringOfNChar(10)
      employee.middleName = generateStringOfNChar(10)
      employee.lastName = generateStringOfNChar(10)
      employee.address = generateStringOfNChar(30)
      employee.pincode = generatePincode(2)
      employee.department = "tech"
      employee.team = generateTeamProjectName(TEAM_LIST)
      employee.project = generateTeamProjectName(PROJECT_LIST)
      employee.sex = false
      employee.ctc = new java.math.BigDecimal(generateDoubleOfN(100000, 1000000), new MathContext(10)) //precision = 6, scala = 4
      employee.monthlySalary = generateDoubleOfN(10000, 1000000)
      employee.createdTime = new Timestamp(System.currentTimeMillis)
      employee.createdDate = new Date(System.currentTimeMillis)
      employeeDetails.add(employee)
    }
    spark.createDataFrame(employeeDetails, classOf[Employee])
  }

  def generateStringOfNChar(number: Int): String = {
    RandomStringUtils.randomAlphabetic(number)
  }

  def generateLongOfN(number: Int): Long = {
    RandomStringUtils.randomNumeric(number).toLong
  }

  def generatePincode(lastRandomDigit: Int): Int = {
    val constantPincode: String = "1234"
    (constantPincode + RandomStringUtils.randomNumeric(2)).toInt
  }

  def generateIntOfN(number: Int): Int = {
    RandomStringUtils.randomNumeric(number).toInt
  }

  def generateTeamProjectName(list: List[String]): String = {
    var n = generateIntOfN(1)
    while (n >= list.length) {
      n = generateIntOfN(1)
    }
    list(n)
  }

  def generateDoubleOfN(startInclusive: Int, endInclusive: Int): Double = {
    RandomUtils.nextDouble(startInclusive, endInclusive)
  }
}
