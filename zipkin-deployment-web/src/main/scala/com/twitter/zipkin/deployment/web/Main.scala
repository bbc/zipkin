package com.twitter.zipkin.deployment.web

import com.twitter.zipkin.conversions.thrift._
import com.twitter.finagle.Http
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.server.{Closer, TwitterServer}
import com.twitter.util.{Await, Closable, Future}
import com.twitter.zipkin.anormdb.AnormDBSpanStoreFactory
import com.twitter.zipkin.zookeeper.ZooKeeperClientFactory
import com.twitter.zipkin.web.ZipkinWebFactory
import com.twitter.zipkin.query.ThriftQueryService
import com.twitter.zipkin.query.constants.DefaultAdjusters

object Main extends TwitterServer with Closer
  with ZooKeeperClientFactory
  with ZipkinWebFactory
  with AnormDBSpanStoreFactory
{
  def main() {
    val store = newAnormSpanStore()
    val query = new ThriftQueryService(store, adjusters = DefaultAdjusters)
    val webService = newWebServer(query, statsReceiver.scope("web"))
    val web = Http.serve(webServerPort(), webService)

    val closer = Closable.sequence(web, store)
    closeOnExit(closer)

    println("running web endpoint and ready")
    Await.all(web, store)
  }
}
