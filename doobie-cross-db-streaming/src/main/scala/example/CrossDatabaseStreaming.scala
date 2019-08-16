package example

import java.sql.Connection

import cats.data.Kleisli
import cats.effect.IO
import cats.~>
import doobie.{ConnectionIO, Transactor}
import doobie.implicits._
import fs2.Stream

object CrossDatabaseStreaming {
  type KleisliPartial[A] = Kleisli[IO, Connection, A]

  val ioToKleisli = new ~>[IO, KleisliPartial] {
    def apply[A](fa: IO[A]): KleisliPartial[A] = Kleisli(_ => fa)
  }

  /**
   * Adapted from https://github.com/tpolecat/doobie/pull/544/files with some changes:
   * 1. Added ability to transform the source input
   * 2. Removed kind projections for use in ammonite scripts
   * 3. Removed effect polymorphism
   */
  def streamAcrossTransactors[Source, SinkInput, SinkReturn](
    source: Stream[ConnectionIO, Source],
    sink: SinkInput => ConnectionIO[SinkReturn],
    transformSourceInput: Source => SinkInput,
    sourceTransactor: Transactor[IO],
    sinkTransactor: Transactor[IO]
  ): IO[Unit] =
    sinkTransactor.exec.apply {
      source
        .transact(sourceTransactor)
        .translate(ioToKleisli)
        .evalMap[KleisliPartial, SinkReturn] { source =>
          sink(transformSourceInput(source)).foldMap[KleisliPartial](sinkTransactor.interpret)
        }
        .compile
        .drain
    }
}