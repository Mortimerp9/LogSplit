package net.pierreandrews.utils

import java.io.{PrintWriter, File, FileWriter}
import java.text.SimpleDateFormat
import java.util.{Date, UUID}

import scala.util.Random
import scala.util.control.NonFatal

import com.quantifind.sumac.{ArgMain, FieldArgs}
import com.quantifind.sumac.validation.Required

/**
 * Generate sample logs
 * User: pierre
 * Date: 11/28/14
 */
object LogGenerator extends ArgMain[LogGenArgs] {

  //generate dates of the format: 15/Aug/2013:13:54:38 -0300
  val dateformat = new SimpleDateFormat("dd/MMM/YYYY:HH:mm:ss Z")

  /**
   * generate random sequential timestamps. Start now and goes backward. This will break if you
   * really ask for too many timestamps and go past the 0 epoch
   */
  def tsGenerator(): Iterator[String] = Iterator.iterate(System.currentTimeMillis) { previousTS =>
    val dec = Random.nextInt(3600) * 1000L
    previousTS - dec
  }.map { ts =>
    dateformat.format(new Date(ts))
  }

  override def main(args: LogGenArgs): Unit = {
    /**
     * generate a fixed number of user ids and repeat it randomly
     */
    val uids: Iterator[String] = {
      val ids: Seq[String] = for (i <- 0 until args.numUsers) yield { UUID.randomUUID().toString }
      Iterator.continually {
        val randIdx = Random.nextInt(ids.size)
        ids(randIdx)
      }
    }

    /**
     * randomly generate errors in the logs
     */
    def makeError: Boolean = Random.nextDouble() < args.errorRate

    // list of dirs to output to, one per server
    val outputDirs = (0 until args.numServers).map { serverID =>
      new File(args.output, s"server$serverID")
    }

    //create logs for each server
    outputDirs.foreach {
      dir =>
        if(!dir.exists) {
          dir.mkdir()
        }
        //get a new timestamp generator
        val tsGen = tsGenerator()

        //open N files and write to them
        (0 until args.numFiles).foreach {
          fileID =>
            val file = new PrintWriter(new File(dir, s"log.$fileID.log"))
            try {
              //generate M lines per file
              (0 until args.linePerFile).foreach { l =>
                val ts = tsGen.next()
                val uid = uids.next()
                val line = s"""177.126.180.83 - - [$ts] "GET /meme.jpg HTTP/1.1" 200 2148 "-" "userid=$uid""""
                val outline = if(makeError) {
                  line.substring(0, Random.nextInt(line.length)+1)
                } else line
                file.println(outline)
              }
            } catch {
              case NonFatal(e) =>  //TODO
            } finally {
              file.close()
            }
        }

    }

  }

}

class LogGenArgs extends FieldArgs {

  @Required
  var output: File = _
  var numServers = 3
  var linePerFile: Int = 1000
  var numFiles: Int = 10
  var errorRate: Double = 0.001
  var numUsers: Int = 100

  addValidation {
    require(output.exists && output.isDirectory && output.listFiles().isEmpty, "Output has to be an empty directory")
  }
}