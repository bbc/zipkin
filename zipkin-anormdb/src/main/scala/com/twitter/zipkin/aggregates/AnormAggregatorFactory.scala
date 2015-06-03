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
import com.twitter.zipkin.storage.anormdb.{ SpanStoreDB, AnormAggregatesWithSpanStore }

trait AnormAggregatorFactory {
  def newAnormAggregator(db : SpanStoreDB) = {
    new AnormAggregator(db, new AnormAggregatesWithSpanStore(db))
  }
}
