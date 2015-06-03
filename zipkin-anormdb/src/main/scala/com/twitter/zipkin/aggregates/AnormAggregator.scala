/*
 * Copyright 2014 Twitter Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.zipkin.aggregates.anormdb

import anorm.SqlParser._
import anorm._
import com.twitter.util.{Duration, Future, FuturePool, Time, Try}
import com.twitter.zipkin.Constants
import com.twitter.zipkin.common._
import com.twitter.zipkin.storage.{IndexedTraceId, SpanStore, TraceIdDuration}
import com.twitter.zipkin.util.Util
import com.twitter.algebird.{ Moments, Monoid, MomentsGroup }
import java.nio.ByteBuffer
import java.sql.{Connection, PreparedStatement}
import com.twitter.zipkin.storage.anormdb.SpanStoreDB
import com.twitter.zipkin.storage.Aggregates

class AnormAggregator(db : SpanStoreDB, aggregates : Aggregates) {

  case class SpanSummary( minTime : Long, maxTime : Long, spanCount : Long ){
    val stepSize = ((maxTime - minTime) * Math.min(10000.0 / spanCount, 1)).toLong
    val iterator = Range.Long(minTime, maxTime+1, stepSize).iterator
  }

  def apply() : Future[Unit] = {
    getSpanSummary() flatMap { 
      case Some(spanSummary) =>
        println(s"Aggregating ${spanSummary.spanCount} spans with step size ${spanSummary.stepSize}")
        val rangeIter = spanSummary.iterator
        val start = rangeIter.next
        calculateDependencies( start, rangeIter, Future.value(Monoid.zero[Dependencies]) )
      case None =>
        Future.Unit
    } flatMap {
      case dependencies : Dependencies =>
          println(s"Storing $dependencies")
      aggregates.storeDependencies(dependencies)
      case _ =>
        println("No new span dependencies to store")
        Future.Unit
    }
  }

  def getSpanSummary() : Future[Option[SpanSummary]] = db.withConnection {
    implicit conn : Connection =>
    val res = SQL("""
     |SELECT min(created_ts) as minTime, max(created_ts) as maxTime, count(*) as spanCount 
     |FROM zipkin_spans 
     |WHERE created_ts > (SELECT IFNULL(MAX(END_TS), 0) FROM zipkin_dependencies) 
    """.stripMargin).apply().head
    if (res[Long]("spanCount") > 0){
      Some(SpanSummary(res[Long]("minTime"), res[Long]("maxTime"), res[Long]("spanCount")))
    } else {
      None
    }
  }

  private[this] def byTimeRangeDependencyLinksSql(startTimeNanos : Long, endTimeNanos : Long) = SQL("""
    |SELECT c.service_name AS child_service_name,p.service_name AS parent_service_name,s.duration AS duration, s.created_ts AS created_ts 
    |FROM zipkin_spans s join zipkin_annotations p join zipkin_annotations c 
    |WHERE s.span_id=c.span_id 
    |AND s.parent_id=p.span_id
    |AND created_ts > (%d) and created_ts <= (%d)
  """.stripMargin.format(startTimeNanos, endTimeNanos))

  private[this] val dependencyLinksResults = (
    get[String](1) ~
    get[String](2) ~
    get[Option[Long]]("duration")
  ) map { case parent~child~duration =>
      val moments = duration.map { d => Moments(d.toDouble) }.getOrElse(Monoid.zero[Moments])
      DependencyLink(Service(parent), Service(child), moments)
  }

  def getDependencyLinks(startTimeNanos : Long, endTimeNanos : Long) : Future[Seq[DependencyLink]] = db.withConnection {
    implicit conn : Connection =>
    try {
        byTimeRangeDependencyLinksSql(startTimeNanos, endTimeNanos).as(dependencyLinksResults *)
    } catch {
      case e : Throwable => println(e); throw e;
    }
  }

  def calculateDependencies( startTimeNanos : Long, timeRangeIter : Iterator[Long], dependencies : Future[Dependencies] ) : Future[Dependencies] = {
    import DependencyLink._
    if (timeRangeIter.hasNext){
      val endTimeNanos = timeRangeIter.next
      getDependencyLinks(startTimeNanos, endTimeNanos) flatMap { dlinks =>
        val summedDependencyLinks = mergeDependencyLinks(dlinks)
        val updatedDependencies = dependencies.map { mergeDependenciesForTimeRange(startTimeNanos, endTimeNanos, summedDependencyLinks.toSeq, _) }
        println(s"Updated dependencies are $updatedDependencies")
        calculateDependencies(endTimeNanos, timeRangeIter, updatedDependencies)
      }
    } else {
      println("No more ranges in time range")
      dependencies
    }
  }

  private[this] def mergeDependenciesForTimeRange(startTimeNanos : Long, endTimeNanos : Long, dlinks : Seq[DependencyLink], dependencies : Dependencies) : Dependencies = {
    import Dependencies._
    val currentTimeRangeDeps = Dependencies(Time.fromMicroseconds(startTimeNanos), Time.fromMicroseconds(endTimeNanos), dlinks)
    monoid.plus(dependencies, currentTimeRangeDeps)
  }
}
