package net.pierreandrews.utils

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.{Date, UUID}

import scala.util.Random
import scala.util.control.NonFatal

import com.quantifind.sumac.{ArgMain, FieldArgs}
import com.quantifind.sumac.validation.{Positive, Required}

/**
 * Generate sample logs. This is pretty naive but tries to generate logs looking like the instruction, with other assumptions:
 *  - log files are in buckets and not in one monolithic file
 *  - each bucket is sorted, with the top line having the max date and bottom line the min date
 *  - each bucket is numbered from 0 to N, bucket 0 has the max date at the top, the N bucket as the min date at the bottom
 *  - buckets are named with the pattern log.X.log
 *
 * See LogGenArgs for possible command line arguments.
 *
 * Random noise is generated to create invalid lines (again, naively truncates the end of some lines)
 *
 * Dates are generated from now in a decreasing order, at random distances one from the other (but always going towards the
 * past).
 *
 * We assume a limited set of user ids (to have log lines to group), you can specify this on the command line. Then each log
 * line is assigned a random user from this limited set, this means that at the end, you might have less users in the generated
 * logs than requested.
 *
 * User: pierre
 * Date: 11/28/14
 */
object LogGenerator extends ArgMain[LogGenArgs] {

  //generate dates of the format: 15/Aug/2013:13:54:38 -0300
  val dateformat = new SimpleDateFormat("dd/MMM/YYYY:HH:mm:ss Z")

  /*
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
    /*
     * generate a fixed number of user ids and repeat it randomly
     */
    val uids: Iterator[String] = {
      val ids: Seq[String] = for (i <- 0 until args.numUsers) yield { UUID.randomUUID().toString }
      Iterator.continually {
        val randIdx = Random.nextInt(ids.size)
        ids(randIdx)
      }
    }

    /*
     * randomly generate errors in the logs
     */
    def makeError: Boolean = Random.nextDouble() < args.errorRate

    // list of dirs to output to, one per server
    val outputDirs = (0 until args.numServers).map { serverID =>
      // these are generated locally, if you want to simulate on separate physical
      // servers, you'll have to copy the files over
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
                // except for the date and userid, the rest of the line content is the same
                val line = s"""177.126.180.83 - - [$ts] "GET /meme.jpg HTTP/1.1" 200 2148 "-" "userid=$uid""""
                // maybe create som error
                val outline = if(makeError) {
                  line.substring(0, Random.nextInt(line.length)+1)
                } else line
                file.println(outline)
              }
            } catch {
              case NonFatal(e) =>
                e.printStackTrace()
                System.exit(-1)
            } finally {
              file.close()
            }
        }

    }

  }

}

/**
 * command line arguments, using Sumac
 */
class LogGenArgs extends FieldArgs {

  @Required
  var output: File = _
  //sets the number of output folders
  @Positive
  var numServers = 3
  @Positive
  var linePerFile: Int = 1000
  //number of buckets
  @Positive
  var numFiles: Int = 10
  //how often should an error be generated
  @Positive
  var errorRate: Double = 0.001

  //how many different users do we want
  @Positive
  var numUsers: Int = 100

  addValidation {
    // validate the output directory.
    require(output.exists && output.isDirectory && output.listFiles().isEmpty, "Output has to be an empty directory")
  }
}