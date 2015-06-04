package com.twitter.zipkin.deployment.web

import com.twitter.finagle.stats.{DefaultStatsReceiver, StatsReceiver}
import com.twitter.util.{Await, Closable, Future}
import com.twitter.zipkin.anormdb.AnormDBSpanStoreFactory
import com.twitter.zipkin.aggregates.anormdb.AnormAggregatorFactory
import com.twitter.zipkin.storage.anormdb.SpanStoreDB
import com.twitter.app.App

object AnormAggregatesJobRunner extends AnormAggregatorFactory with App {

  val anormDB = flag("zipkin.storage.anormdb.db", "sqlite::memory:", "JDBC location URL for the AnormDB")

  def main() {

    val db = SpanStoreDB(anormDB())
    val aggregator = newAnormAggregator(db)
    println(s"Running aggregator job")
    val job = aggregator()
    job.rescue {
      case e =>
        println(s"Job failed : $e")
        Future.exception(e)
    }
    Await.all(job)
    println("Finished running aggregates job")
  }
}
