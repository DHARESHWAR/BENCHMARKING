package parquet.speedtest

import java.sql.{Date, Timestamp}

import scala.beans.BeanProperty

@SerialVersionUID(768798076199454511L)
class Employee extends Serializable {

  @BeanProperty
  var id: Int = _
  @BeanProperty
  var uniqueId: Long = _
  @BeanProperty
  var firstName: String = _
  @BeanProperty
  var middleName: String = _
  @BeanProperty
  var lastName: String = _
  @BeanProperty
  var address: String = _
  @BeanProperty
  var pincode: Int = _

  @BeanProperty
  var department: String = _
  @BeanProperty
  var team: String = _
  @BeanProperty
  var project: String = _

  @BeanProperty
  var sex: Boolean = _

  @BeanProperty
  var ctc: java.math.BigDecimal = _
  @BeanProperty
  var monthlySalary: Double = _

  @BeanProperty
  var createdTime: Timestamp = _
  @BeanProperty
  var createdDate: Date = _
}
