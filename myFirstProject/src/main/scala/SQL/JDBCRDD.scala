package SQL
import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry


private  object JDBCRDD {
  def getConnector(driver: String, url: String, properties: Properties): () => Connection = {
    () => {
      try {
        if (driver != null) DriverRegistry.register(driver)
      } catch {
        case e: ClassNotFoundException =>
          System.err.println(s"Couldn't find class $driver", e)
      }
      DriverManager.getConnection(url, properties)
    }
  }
}
