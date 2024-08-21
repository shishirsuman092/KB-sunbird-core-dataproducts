package org.ekstep.analytics.adhoc

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.JsonNode

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.LinkedHashMap
import java.io.{File, PrintWriter}
import scala.io.Source

object JobLogSummary {

  val mapper: ObjectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def main(args: Array[String]): Unit = {
    // Define the path to the log file
    val logFileBasePath = "/mount/data/analytics/scripts/logs/"
    val logFilePath = s"${logFileBasePath}joblog.log"

    // Read the log file and ensure the resource is closed after use
    val logLines = {
      val source = Source.fromFile(logFilePath)
      try {
        source.getLines().toList
      } finally {
        source.close()
      }
    }

    // Group the logs by pid, preserving the order
    val logsGroupedByPid = groupLogsByPid(logLines)

    // Track errors separately
    val errors = logLines.flatMap(parseErrorLogLine)

    // Prepare output
    val output = new StringBuilder

    // Validate each job (grouped by pid)
    logsGroupedByPid.foreach { case (pid, logs) =>
      if (pid == "org.ekstep.analytics.model.WorkFlowSummaryModel") {
        // Add any specific handling for WorkFlowSummaryModel if needed
      } else {
        val hasJobStart = logs.exists(_.eventId == "JOB_START")
        val hasJobEnd = logs.exists(_.eventId == "JOB_END")

        // Other jobs should have exactly 3 events (1 JOB_LOG + JOB_START + JOB_END)
        if (hasJobStart && hasJobEnd) {
          output.append(s"Job '$pid' has JOB_START and JOB_END events.\n")

          // Print both ets and actual DateTime for JOB_START events
          logs.filter(_.eventId == "JOB_START").foreach { startEvent =>
            val etsDateTime = startEvent.ets.map(convertEtsToDateTime).getOrElse("N/A")
            output.append(s"  - '$pid' JOB_START ets: ${startEvent.ets.getOrElse("N/A")} ($etsDateTime)\n")
          }

          // Print both ets and actual DateTime for JOB_END events, along with timeTaken
          logs.filter(_.eventId == "JOB_END").foreach { endEvent =>
            val etsDateTime = endEvent.ets.map(convertEtsToDateTime).getOrElse("N/A")
            output.append(s"  - '$pid' JOB_END ets: ${endEvent.ets.getOrElse("N/A")} ($etsDateTime)\n")
            output.append(s"  - '$pid' timeTaken: ${endEvent.timeTaken.getOrElse("N/A")}\n")
          }

        } else {
          output.append(s"Job with pid '$pid' is missing some events:\n")
          if (!hasJobStart) output.append(s"  - Missing JOB_START\n")
          if (!hasJobEnd) output.append(s"  - Missing JOB_END\n")
        }
      }
    }

    // Print errors
    if (errors.nonEmpty) {
      output.append("==== Errors ====\n")
      errors.foreach { case (pid, message) =>
        output.append(s"PID: $pid, Error: $message\n")
      }
    } else {
      output.append("No errors found.\n")
    }

    // Write output to a file
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateFormat.format(new Date())
    val outputFilePath = s"${logFileBasePath}joblog_summary_$today.txt"
    writeOutputToFile(outputFilePath, output.toString())
  }

  case class LogEntry(
                       eventId: String,
                       pid: String,
                       status: Option[String],
                       ets: Option[String],
                       message: Option[String],
                       inputEvents: Option[Int],
                       outputEvents: Option[Int],
                       timeTaken: Option[Int]
                     )

  private def parseLogLine(line: String): Option[LogEntry] = {
    try {
      val jsonNode: JsonNode = mapper.readTree(line)
      val eventId = jsonNode.get("eid").asText()
      val pid = jsonNode.get("context").get("pdata").get("pid").asText()
      val status = Option(jsonNode.get("edata").get("status")).map(_.asText())
      val ets = Option(jsonNode.get("ets")).map(_.asText())
      val message = Option(jsonNode.get("edata").get("message")).map(_.asText())
      val inputEvents = Option(jsonNode.get("edata").get("data").get("inputEvents")).map(_.asInt())
      val outputEvents = Option(jsonNode.get("edata").get("data").get("outputEvents")).map(_.asInt())
      val timeTaken = Option(jsonNode.get("edata").get("data").get("timeTaken")).map(_.asInt())

      Some(LogEntry(eventId, pid, status, ets, message, inputEvents, outputEvents, timeTaken))
    } catch {
      case _: Exception => None
    }
  }

  private def parseErrorLogLine(line: String): Option[(String, String)] = {
    try {
      val jsonNode: JsonNode = mapper.readTree(line)
      val pid = jsonNode.get("context").get("pdata").get("pid").asText()
      val status = Option(jsonNode.get("edata").get("status")).map(_.asText())
      val message = Option(jsonNode.get("edata").get("data").get("statusMsg")).map(_.asText())

      if (status.contains("FAILED") || status.contains("ERROR")) {
        Some(pid -> message.getOrElse("No error message provided"))
      } else {
        None
      }
    } catch {
      case _: Exception => None
    }
  }

  def groupLogsByPid(logLines: List[String]): LinkedHashMap[String, List[LogEntry]] = {
    val logEntries = logLines.flatMap(parseLogLine)
    val grouped = LinkedHashMap[String, List[LogEntry]]()

    logEntries.foreach { entry =>
      val pid = entry.pid
      if (!grouped.contains(pid)) {
        grouped(pid) = List(entry)
      } else {
        grouped(pid) = grouped(pid) :+ entry
      }
    }

    grouped
  }

  private def convertEtsToDateTime(ets: String): String = {
    val timestamp = ets.toLong
    val date = new Date(timestamp)
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format.format(date)
  }

  private def writeOutputToFile(filePath: String, content: String): Unit = {
    val file = new File(filePath)
    val writer = new PrintWriter(file)
    try {
      writer.write(content)
    } finally {
      writer.close()
    }
  }
}