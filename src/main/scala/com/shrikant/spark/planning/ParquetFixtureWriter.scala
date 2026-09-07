package com.shrikant.spark.planning

import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.io.{OutputFile, PositionOutputStream}
import org.apache.parquet.schema.MessageTypeParser

import java.io.RandomAccessFile
import java.nio.file.{Files, Paths, Path => NioPath}

object ParquetFixtureWriter {

  private val schema =
    MessageTypeParser.parseMessageType(
      "message spark_pushdown_fixture {" +
        " required int64 id;" +
        " required int64 customer_id;" +
        " required binary customer_name (UTF8);" +
        " required int64 amount;" +
        "}"
    )

  private final class LocalOutputFile(path: NioPath)
    extends OutputFile {

    override def create(blockSizeHint: Long): PositionOutputStream =
      createOrOverwrite(blockSizeHint)

    override def createOrOverwrite(
                                    blockSizeHint: Long
                                  ): PositionOutputStream = {

      Files.createDirectories(path.getParent)

      new PositionOutputStream {

        private val file =
          new RandomAccessFile(path.toFile, "rw")

        file.setLength(0L)

        override def write(b: Int): Unit =
          file.write(b)

        override def write(
                            b: Array[Byte],
                            off: Int,
                            len: Int
                          ): Unit =
          file.write(b, off, len)

        override def getPos: Long =
          file.getFilePointer

        override def close(): Unit =
          file.close()
      }
    }

    override def supportsBlockSize(): Boolean =
      false

    override def defaultBlockSize(): Long =
      0L

    override def getPath: String =
      path.toAbsolutePath.toString
  }

  def write(
             outputPath: String,
             rows: Int
           ): Unit = {

    val path =
      Paths.get(outputPath)

    if (Files.exists(path)) {
      Files.delete(path)
    }

    val outputFile =
      new LocalOutputFile(path)

    val writer =
      ExampleParquetWriter
        .builder(outputFile)
        .withType(schema)
        .build()

    val factory =
      new SimpleGroupFactory(schema)

    try {

      var id = 0

      while (id < rows) {

        val customerId =
          id % 100

        val group =
          factory
            .newGroup()
            .append("id", id.toLong)
            .append("customer_id", customerId.toLong)
            .append(
              "customer_name",
              s"customer_$customerId"
            )
            .append(
              "amount",
              id.toLong * 10L
            )

        writer.write(group)

        id += 1
      }

    } finally {
      writer.close()
    }
  }

  def main(args: Array[String]): Unit = {

    val outputPath =
      if (args.nonEmpty)
        args(0)
      else
        "build/module-1.4.11/parquet/fixture.parquet"

    val rows =
      if (args.length > 1)
        args(1).toInt
      else
        100000

    println(s"Writing Parquet fixture: $outputPath")
    println(s"Rows: $rows")

    write(outputPath, rows)

    println(s"Parquet fixture created successfully: $outputPath")
  }
}