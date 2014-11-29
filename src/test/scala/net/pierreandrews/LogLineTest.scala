package net.pierreandrews

import java.util.Date

import org.scalatest._

/**
 * TODO DOC
 * User: pierre
 * Date: 11/28/14
 */
class LogLineTest extends FunSuite with Matchers {

  //very basic unit test based on example.
  test("parse line") {
    val lines = Seq("""177.126.180.83 - - [15/Aug/2013:13:54:38 -0300] "GET /meme.jpg HTTP/1.1" 200 2148 "-" "userid=5352b590-05ac-11e3-9923-c3e7d8408f3a"""",
      """177.126.180.83 - - [15/Aug/2013:13:54:38 -0300] "GET /lolcats.jpg HTTP/1.1" 200 5143 "-" "userid=f85f124a-05cd-11e3-8a11-a8206608c529"""",
      """177.126.180.83 - - [15/Aug/2013:13:57:48 -0300] "GET /lolcats.jpg HTTP/1.1" 200 5143 "-" "userid=5352b590-05ac-11e3-9923-c3e7d8408f3a"""")

    val expected = Seq(new LogLine("5352b590-05ac-11e3-9923-c3e7d8408f3a", new Date(1376585678),
      """177.126.180.83 - - [15/Aug/2013:13:54:38 -0300] "GET /meme.jpg HTTP/1.1" 200 2148 "-" "userid=5352b590-05ac-11e3-9923-c3e7d8408f3a"""", 3),
    new LogLine("f85f124a-05cd-11e3-8a11-a8206608c529", new Date(1376585678),
      """177.126.180.83 - - [15/Aug/2013:13:54:38 -0300] "GET /lolcats.jpg HTTP/1.1" 200 5143 "-" "userid=f85f124a-05cd-11e3-8a11-a8206608c529"""", 3),
    new LogLine("5352b590-05ac-11e3-9923-c3e7d8408f3a", new Date(1376585868),
      """177.126.180.83 - - [15/Aug/2013:13:57:48 -0300] "GET /lolcats.jpg HTTP/1.1" 200 5143 "-" "userid=5352b590-05ac-11e3-9923-c3e7d8408f3a"""", 3))

    lines.map(LogLine(_, 3)).flatten should contain only(expected:_*)

  }
}
