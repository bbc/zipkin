package com.twitter.zipkin.deployment.web

import com.twitter.zipkin.conversions.thrift._
import com.twitter.finagle.Http
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.DefaultTimer
import com.twitter.conversions.time._
import com.twitter.server.{Closer, TwitterServer}
import com.twitter.util.{Timer, Time, Duration, Await, Closable, Future}
import com.twitter.zipkin.anormdb.AnormDBSpanStoreFactory
import com.twitter.zipkin.zookeeper.ZooKeeperClientFactory
import com.twitter.zipkin.web.ZipkinWebFactory
import com.twitter.zipkin.query.ThriftQueryService
import com.twitter.zipkin.query.constants.DefaultAdjusters
import com.twitter.zipkin.storage.anormdb.AnormAggregatesWithSpanStore
import com.twitter.zipkin.aggregates.anormdb.{ AnormAggregator, AnormAggregatorFactory }

object Main extends TwitterServer with Closer
  with ZooKeeperClientFactory
  with ZipkinWebFactory
  with AnormDBSpanStoreFactory
  with AnormAggregatorFactory
{
  private[this] def startAggregator(aggregator : AnormAggregator, timer :Timer = DefaultTimer.twitter) =  timer.schedule(Time.now, 1.hours) {
    aggregator().rescue {
      case e =>
        log.error(s"Aggregator job failed $e", e)
        Future.Unit
    }
  }
  
  def main() {
    val storeDB = newSpanStoreDB()
    val store = newAnormSpanStore(storeDB)
    val anormAggregates = new AnormAggregatesWithSpanStore(storeDB)
    val aggregatorTask = startAggregator(newAnormAggregator(storeDB))
    val query = new ThriftQueryService(store, aggsStore = anormAggregates, adjusters = DefaultAdjusters)
    val webService = newWebServer(query, statsReceiver.scope("web"))
    val web = Http.serve(webServerPort(), webService)

    val closer = Closable.sequence(web, store, aggregatorTask)
    closeOnExit(closer)

    log.info("running web endpoint and ready")
    Await.all(web, store)
  }
}
