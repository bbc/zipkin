package com.twitter.mycollector

import com.twitter.zipkin.conversions.thrift._
import com.twitter.finagle.Http
import com.twitter.finagle.stats.{DefaultStatsReceiver, StatsReceiver}
import com.twitter.server.{Closer, TwitterServer}
import com.twitter.util.{Await, Closable, Future}
import com.twitter.zipkin.anormdb.AnormDBSpanStoreFactory
import com.twitter.zipkin.collector.{ SpanReceiver, ZipkinCollectorFactory }
import com.twitter.zipkin.storage.{ WriteSpanStore, SpanStore }
import com.twitter.zipkin.receiver.scribe.ScribeSpanReceiverFactory
import com.twitter.zipkin.sampler.{Sampler, AdaptiveSampler, SpanSamplerFilter}
import com.twitter.zipkin.thriftscala.Span
import com.twitter.zipkin.zookeeper.ZooKeeperClientFactory
import com.twitter.util.Var

object Main extends TwitterServer with Closer
  with ZooKeeperClientFactory
  with ScribeSpanReceiverFactory
  with AnormDBSpanStoreFactory
  with ZipkinCollectorFactory
  with AdaptiveSampler
{

  val sampleRate = flag("zipkin.storage.samplerate", 1.0, "Sample Rate between 0 and 1")
  val useAdaptiveSampler = flag("zipkin.storage.adaptive", false, "Use adaptive sampler")

  override def newReceiver(receive: Seq[Span] => Future[Unit], stats: StatsReceiver): SpanReceiver =   newScribeSpanReceiver( receive, stats )

  override def newSpanStore(stats : StatsReceiver) : WriteSpanStore = newAnormSpanStore()

  override def spanStoreFilter: SpanStore.Filter = {
    if (useAdaptiveSampler()){
      log.info("Using adaptive sampler")
      newAdaptiveSamplerFilter()
    } else {
      log.info(s"Using fixed sampler with rate ${sampleRate()}")
      new SpanSamplerFilter(new Sampler(Var(sampleRate())))
    }
  }

  def main() {
    val collector = newCollector(statsReceiver.scope("SpanCollector"))
    closeOnExit(collector)
    log.info("Running collector ready")
    Await.all(collector)
  }
}
