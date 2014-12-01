package net.pierreandrews

import org.joda.time.DateTime
import org.scalatest._

/**
 * Basic test for the parser, uses the examples from the instructions
 *
 * User: pierre
 * Date: 11/28/14
 */
class ParserTest extends FunSuite with Matchers {

  //very basic unit test based on example.
  test("parse line") {
    val lines = Seq("""177.126.180.83 - - [15/Aug/2013:13:54:38 -0300] "GET /meme.jpg HTTP/1.1" 200 2148 "-" "userid=5352b590-05ac-11e3-9923-c3e7d8408f3a"""",
      """177.126.180.83 - - [15/Aug/2013:14:54:38 -0300] "GET /lolcats.jpg HTTP/1.1" 200 5143 "-" "userid=f85f124a-05cd-11e3-8a11-a8206608c529"""",
      """177.126.180.83 - - [15/Aug/2013:13:57:48 -0300] "GET /lolcats.jpg HTTP/1.1" 200 5143 "-" "userid=5352b590-05ac-11e3-9923-c3e7d8408f3b"""",
      """177.126.180.83 - - [1513:57:48 -0300] "GET /lolcats.jpg HTTP/1.1" 200 5143 "-" "userid=5352b590-05ac-11e3-9923-345235324"""",
      """177.126.180.83 - - [16/Dec/2013:13:57:48 -0300] "GET /lolcats.jpg HTTP/1.1" 200 5143 "-" "userid=5352b590-05ac-1""")

    val expectedDates = Seq(new DateTime(1376585678000L), new DateTime(1376589278000L), new DateTime(1376585868000L), new DateTime(1387213068000L))

    val expectedUsers = Seq("5352b590-05ac-11e3-9923-c3e7d8408f3a",
      "f85f124a-05cd-11e3-8a11-a8206608c529",
      "5352b590-05ac-11e3-9923-c3e7d8408f3b",
      "5352b590-05ac-11e3-9923-345235324")

    lines.map(Parser.extractDate).flatten should contain only (expectedDates: _*)
    lines.map(Parser.extractUser).flatten should contain only (expectedUsers: _*)

  }
}
