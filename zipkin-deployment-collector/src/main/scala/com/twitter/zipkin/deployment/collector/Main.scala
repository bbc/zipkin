package com.twitter.mycollector

import com.twitter.zipkin.conversions.thrift._
import com.twitter.finagle.Http
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.server.{Closer, TwitterServer}
import com.twitter.util.{Await, Closable, Future}
import com.twitter.zipkin.anormdb.AnormDBSpanStoreFactory
import com.twitter.zipkin.collector.SpanReceiver
import com.twitter.zipkin.common.Span
import com.twitter.zipkin.{thriftscala => thrift}
import com.twitter.zipkin.receiver.scribe.ScribeSpanReceiverFactory
import com.twitter.zipkin.zookeeper.ZooKeeperClientFactory

object Main extends TwitterServer with Closer
  with ZooKeeperClientFactory
  with ScribeSpanReceiverFactory
  with AnormDBSpanStoreFactory
{
  def main() {
    val store = newAnormSpanStore()
    val convert: Seq[thrift.Span] => Seq[Span] = { _.map(_.toSpan) }
    val receiver = newScribeSpanReceiver(convert andThen store, statsReceiver.scope("scribeSpanReceiver"))
    val closer = Closable.sequence(receiver, store)
    closeOnExit(closer)

    println("running receiver ready")
    Await.all(receiver, store)
  }
}
