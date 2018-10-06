package fs2.io.file

import java.io.File
import java.nio.file.{Files, Paths}

import scala.sys.process._
import cats.effect.{IO, Sync}
import fs2.async.mutable.Signal
import fs2.io.file
import fs2.{Fs2Spec, Stream, async, io, text}
import org.scalacheck.Gen

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import cats.effect.IO.timer

class TailSpec extends Fs2Spec {

  def stopAfter[A](f: FiniteDuration): Stream[IO, A] => Stream[IO, A] =
    in => {
      def close(s: Signal[IO, Boolean]) =
        Stream.sleep_[IO](f) ++ Stream.eval(s.set(true))

      Stream.eval(async.signalOf[IO, Boolean](false)).flatMap { end: Signal[IO, Boolean] =>
        in.interruptWhen[IO](end).concurrently(close(end))
      }
    }

  "tail" - {
    "reads an existing file" in {
      val path = Paths.get("/Users/jpablo/tmp/spark.scala")
      val readAllStream = file
        .readAll[IO](path, 4096)
        .through(text.utf8Decode)
        .through(text.lines)
        .compile.toList.unsafeRunSync

      val tailStream = file
        .tail[IO](path)
        .through(text.utf8Decode)
        .through(text.lines)
        .through(stopAfter(100.millis))
        .compile.toList.unsafeRunSync()

      assert(readAllStream.init == tailStream)
    }

    "supports reading a file" in {
      forAll(arrayGen, minSuccessful(10)) { written: List[String] =>
        val runLines = for {
          t <- createSchedulerAndLogReader
          (terminate, readLines, file) = t
          writeLines = delayedWrites(written, file, 100.millis)
            .onComplete(Stream.sleep(1.seconds) ++ Stream.eval(terminate.set(true)))
          runTest = readLines
            .concurrently(writeLines)
            .interruptWhen(terminate)
            .compile
            .toList
          lines <- Stream.eval(runTest)
        } yield lines

        val lines = runLines.compile.toList.unsafeRunSync()

        assert(lines == written.filterNot(_.isEmpty).map(_.getBytes.toList))
      }
    }
  }

  // simulate a slow process by adding a pause between writes
  def delayedWrites(lines: List[String], file: File, pause: FiniteDuration): Stream[IO, Unit] =
    Stream.emits(lines).evalMap(appendTo[IO](file)).interleave(Stream.sleep(pause).repeat)

  //  Generate between 0 and 5 lines of random length
  val arrayGen: Gen[List[String]] =
    for {
      n <- Gen.choose(0, 5)
      lines <- Gen.listOfN(n, Gen.alphaStr)
    } yield lines

  def appendTo[F[_]](file: File, newLine: Boolean = true)(s: String)(implicit F: Sync[F]): F[Unit] =
    F.delay({ (s"""echo ${if (newLine) "" else "-n"} $s""" #>> file).!; () })

  def tmpFile =
    IO(Files.createTempFile("StreamingLogReaderTest", ".tmp").toFile)

  def createSchedulerAndLogReader: Stream[IO, (Signal[IO, Boolean], Stream[IO, Byte], File)] =
    for {
      tmp <- Stream.eval(tmpFile)
      terminate <- Stream.eval(async.signalOf[IO, Boolean](false))
      readLines = file.tail[IO](tmp.toPath)
    } yield (terminate, readLines, tmp)

}
