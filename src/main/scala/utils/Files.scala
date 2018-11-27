package utils

import java.io.FileWriter

object Files {

  def writeResultsToFile(path: String, data: String, accuracy: Double, time: Long, name: String, partitions: Int): Unit = {
    val fw = new FileWriter(path, true)
    try {
      fw.write(s"$data, $name, $partitions, $accuracy, $time\n")
    }
    finally fw.close()
  }

}
