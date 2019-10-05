package org.allenai.pdffigures2

import java.io.File
import java.io.FileInputStream
import java.util.concurrent.atomic.AtomicInteger

import ch.qos.logback.classic.{ Level, Logger }
import org.allenai.common.Logging
import org.allenai.pdffigures2.FigureExtractor.DocumentWithSavedFigures
import org.allenai.pdffigures2.JsonProtocol._
import org.apache.pdfbox.pdmodel.PDDocument
import org.slf4j.LoggerFactory

import scala.collection.parallel.ForkJoinTaskSupport
import io.javalin.Javalin
import io.javalin.core.util.FileUtil
import scala.collection.JavaConverters;
import java.util.ArrayList;
import java.util.LinkedList;
import scala.collection.mutable.MutableList


/** CLI tools to parse a batch of PDFs, and then save the figures, table, captions
  * or text to disk.
  */
object FigureExtractorBatchApi extends Logging {

  case class ProcessingStatistics(
    filename: String,
    numPages: Int,
    numFigures: Int,
    timeInMillis: Long
  )
  case class ProcessingError(filename: String, msg: Option[String], className: String)
  implicit val processingStatisticsFormat = jsonFormat4(ProcessingStatistics.apply)
  implicit val processingErrorFormat = jsonFormat3(ProcessingError.apply)

  case class ApiConfigBatch(
    inputFiles: Seq[File] = Seq(),
    figureDataPrefix: Option[String] = None,
    dpi: Int = 150,
    ignoreErrors: Boolean = false,
    saveStats: Option[String] = None,
    saveRegionlessCaptions: Boolean = false,
    threads: Int = 1,
    debugLogging: Boolean = true,
    fullTextPrefix: Option[String] = None,
    figureImagePrefix: Option[String] = None,
    figureFormat: String = "png"
  )

  def getFilenames(
    prefix: String,
    docName: String,
    format: String,
    figures: Seq[Figure]
  ): Seq[String] = {
    val namesUsed = scala.collection.mutable.Map[String, Int]()
    figures.map { fig =>
      val figureName = s"${fig.figType}${fig.name}"
      namesUsed.update(figureName, namesUsed.getOrElse(figureName, 1))
      val id = namesUsed(figureName)
      val filename = s"$prefix$docName-$figureName-$id.$format"
      filename
    }
  }

  def saveRasterizedFigures(
    prefix: String,
    docName: String,
    format: String,
    dpi: Int,
    figures: Seq[RasterizedFigure],
    doc: PDDocument
  ): Seq[SavedFigure] = {
    val filenames = getFilenames(prefix, docName, format, figures.map(_.figure))
    FigureRenderer.saveRasterizedFigures(filenames.zip(figures), format, dpi)
  }

  def processFile(
    inputFile: File,
    config: ApiConfigBatch
  ): Either[ProcessingError, ProcessingStatistics] = {
    val fileStartTime = System.nanoTime()
    var doc: PDDocument = null
    val figureExtractor = FigureExtractor()
    try {
      doc = PDDocument.load(inputFile)
      val useCairo = FigureRenderer.CairoFormat.contains(config.figureFormat)
      val inputName = inputFile.getName
      val truncatedName = inputName.substring(0, inputName.lastIndexOf('.'))
      val numFigures = if (config.fullTextPrefix.isDefined) {
        val outputFilename = s"${config.fullTextPrefix.get}$truncatedName.json"
        val numFigures = if (config.figureImagePrefix.isDefined && !useCairo) {
          val document = figureExtractor.getRasterizedFiguresWithText(doc, config.dpi)
          val savedFigures = saveRasterizedFigures(
            config.figureImagePrefix.get,
            truncatedName,
            config.figureFormat,
            config.dpi,
            document.figures,
            doc
          )
          val documentWithFigures =
            DocumentWithSavedFigures(savedFigures, document.abstractText, document.sections)
          FigureRenderer.saveAsJSON(outputFilename, documentWithFigures)
          document.figures.size
        } else {
          val document = figureExtractor.getFiguresWithText(doc)
          if (useCairo) {
            val filenames = getFilenames(
              config.figureImagePrefix.get,
              truncatedName,
              config.figureFormat,
              document.figures
            )
            val savedFigures = FigureRenderer
              .saveFiguresAsImagesCairo(
                doc,
                filenames.zip(document.figures),
                config.figureFormat,
                config.dpi
              )
              .toSeq
            val savedDocument =
              DocumentWithSavedFigures(savedFigures, document.abstractText, document.sections)
            FigureRenderer.saveAsJSON(outputFilename, savedDocument)
          } else {
            FigureRenderer.saveAsJSON(outputFilename, document)
          }
          document.figures.size
        }
        numFigures
      } else {
        val (figures, failedCaptions) = if (config.figureImagePrefix.isDefined && !useCairo) {
          val figuresWithErrors = figureExtractor.getRasterizedFiguresWithErrors(doc, config.dpi)
          val savedFigures = saveRasterizedFigures(
            config.figureImagePrefix.get,
            truncatedName,
            config.figureFormat,
            config.dpi,
            figuresWithErrors.figures,
            doc
          )
          (Left(savedFigures), figuresWithErrors.failedCaptions)
        } else {
          val figuresWithErrors = figureExtractor.getFiguresWithErrors(doc)
          if (useCairo) {
            val filenames = getFilenames(
              config.figureImagePrefix.get,
              truncatedName,
              config.figureFormat,
              figuresWithErrors.figures
            )
            val savedFigures = FigureRenderer
              .saveFiguresAsImagesCairo(
                doc,
                filenames.zip(figuresWithErrors.figures),
                config.figureFormat,
                config.dpi
              )
              .toSeq
            (Left(savedFigures), figuresWithErrors.failedCaptions)
          } else {
            (Right(figuresWithErrors.figures), figuresWithErrors.failedCaptions)
          }
        }
        if (config.figureDataPrefix.isDefined) {
          val outputFilename = s"${config.figureDataPrefix.get}$truncatedName.json"
          if (config.saveRegionlessCaptions) {
            val toSave: Map[String, Either[Either[Seq[SavedFigure], Seq[Figure]], Seq[Caption]]] =
              Map(
                "figures" -> Left(figures),
                "regionless-captions" -> Right(failedCaptions)
              )
            FigureRenderer.saveAsJSON(outputFilename, toSave)
          } else {
            val toSave: Either[Seq[SavedFigure], Seq[Figure]] = figures
            FigureRenderer.saveAsJSON(outputFilename, toSave)
          }
        }
        figures match {
          case Left(savedFigures) => savedFigures.size
          case Right(figs) => figs.size
        }
      }
      val timeTaken = System.nanoTime() - fileStartTime
      logger.info(s"Finished ${inputFile.getName} in ${(timeTaken / 1000000) / 1000.0} seconds")
      Right(
        ProcessingStatistics(
          inputFile.getAbsolutePath,
          doc.getNumberOfPages,
          numFigures,
          timeTaken / 1000000
        )
      )
    } catch {
      case e: Exception =>
        if (config.ignoreErrors) {
          logger.info(s"Error: $e on document ${inputFile.getName}")
          Left(
            ProcessingError(
              inputFile.getAbsolutePath,
              Option(e.getMessage),
              e.getClass.getName
            )
          )
        } else {
          throw e
        }
    } finally {
      if (doc != null) doc.close()
    }
  }

  def run(config: ApiConfigBatch): Unit = {
    val startTime = System.nanoTime()
    if (!config.debugLogging) {
      val root = LoggerFactory.getLogger("root").asInstanceOf[Logger]
      root.setLevel(Level.INFO)
    }
    val results = if (config.threads == 1) {
      config.inputFiles.zipWithIndex.map {
        case (inputFile, fileNum) =>
          logger.info(
            s"Processing file ${inputFile.getName} " +
              s"(${fileNum + 1} of ${config.inputFiles.size})"
          )
          processFile(inputFile, config)
      }
    } else {
      val parFiles = config.inputFiles.par
      if (config.threads != 0) {
        parFiles.tasksupport = new ForkJoinTaskSupport(
          new scala.concurrent.forkjoin.ForkJoinPool(config.threads)
        )
      }
      val onPdf = new AtomicInteger(0)
      parFiles.map { inputFile =>
        val curPdf = onPdf.incrementAndGet();
        logger.info(
          s"Processing file ${inputFile.getName} " +
            s"($curPdf of ${config.inputFiles.size})"
        )
        processFile(inputFile, config)
      }.toList
    }
    val totalTime = System.nanoTime() - startTime
    logger.info(s"Finished processing ${config.inputFiles.size} files")
    logger.info(s"Took ${(totalTime / 1000000) / 1000.0} seconds")

    if (config.saveStats.isDefined) {
      FigureRenderer.saveAsJSON(config.saveStats.get, results)
      logger.info(s"Stats saved to ${config.saveStats.get}")
    }
    val errors = results.flatMap {
      case Left(pe) => Some(pe)
      case _ => None
    }
    if (errors.isEmpty) {
      logger.info(s"No errors")
    } else {
      val errorString = errors
        .map {
          case ProcessingError(name, msg, errorName) => s"$name: $errorName: $msg"
        }
        .mkString("\n")
      if (config.saveStats.isDefined) {
        logger.info(s"Errors ${errors.size} files")
      } else {
        logger.info(s"Errors on the following files:\n$errorString")
      }
    }
  }



  def main(args: Array[String]): Unit = {

    val app = Javalin.create().start(7000)
    app.get("/alive", ctx => ctx.result("yes"))   
    app.post("/processPdf", ctx => {
        val workFolder = java.nio.file.Files.createTempDirectory("pdfproc");
        val dataFilePrefix =workFolder + "/"
        val files =  MutableList[File]()
        val uploadedFile = ctx.uploadedFile("file")
        val filePath = workFolder + uploadedFile.getFilename()
        FileUtil.streamToFile(uploadedFile.getContent(),filePath)
        val inputFile = new File(filePath)
        val filesSeq = Seq(inputFile)
        val resultFile = dataFilePrefix + inputFile.getName().replace(".pdf","") + ".json"
        val config = ApiConfigBatch(inputFiles=filesSeq,figureDataPrefix=Option(dataFilePrefix))
        run(config)
        ctx.result( new FileInputStream(new File(resultFile)))
    });


    app.get("/uploadOne", ctx => ctx.html("<html><body><form method='post' action='/processPdf' enctype='multipart/form-data'>"+
    "<label for='file'>Process pdf</label>"+
    "<input type='file' name='file'>"+
    "<button>Submit</button></form></body></html>"));
    /*
    TODO implement batch processing
    app.post("/processPdfs", ctx => {
        val workFolder = "upload/"
        val dataFilePrefix =workFolder + "/"
        val files =  MutableList[File]()
        val resultFileNames = MutableList[String]()
        ctx.uploadedFiles("files").forEach(file => {
            val filePath = workFolder + file.getFilename()
            FileUtil.streamToFile(file.getContent(),filePath)
            val inputFile = new File(filePath)
            files += inputFile
        });
        val filesSeq = Seq(files:_*)
        val config = ApiConfigBatch(inputFiles=filesSeq,figureDataPrefix=Option(dataFilePrefix))
        run(config)
        ctx.result( new FileInputStream(new File(resultFileNames(0))))
    });
    
    app.get("/uploadMore", ctx => ctx.html("<html><body><form method='post' action='/processPdfs' enctype='multipart/form-data'>"+
      "<label for='files'>Process pdf</label>"+
      "<input type='file' name='files' multiple>"+
      "<button>Submit</button></form></body></html>"));
    */

    
  }
}
