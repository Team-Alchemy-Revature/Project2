import java.io.IOException

import scala.util.Try

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

object HiveData {
  def main(args: Array[String]): Unit = {
    println("Loading Data into Hive...")

    demo2()
  }

  def demo2(): Unit = {

  var con: java.sql.Connection = null;
  try {
    var driverName = "org.apache.hive.jdbc.HiveDriver"
    val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/foodmart";

    Class.forName(driverName);

    con = DriverManager.getConnection(conStr, "", "");
    val stmt = con.createStatement();

    println("Executing SELECT * FROM account....")
    var acct = stmt.executeQuery("SELECT * FROM account");
    while (acct.next()) {
      println(s"${acct.getString(1)}")
    }
    System.out.println("Query Executed successfully!");
  }catch {
      case ex => {
        ex.printStackTrace();
        throw new Exception(s"${ex.getMessage}")
      }
    } finally {
      try {
        if (con != null)
          con.close();
      } catch {
        case ex => {
          ex.printStackTrace();
          throw new Exception(s"${ex.getMessage}")
        }
      }
    }
  }
}
