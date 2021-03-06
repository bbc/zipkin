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
package com.twitter.zipkin.anormdb

import com.twitter.app.App
import com.twitter.zipkin.storage.SpanStore
import com.twitter.zipkin.storage.anormdb.{AnormSpanStore, SpanStoreDB}

trait AnormDBSpanStoreFactory { self: App =>

  val anormDB = flag("zipkin.storage.anormdb.db", "sqlite::memory:", "JDBC location URL for the AnormDB")
  val anormInstall = flag("zipkin.storage.anormdb.install", false, "Create the tables")
  val anormConnectionPoolSize = flag("zipkin.storage.anormdb.poolsize", 1, "Connection pool size")


  def newSpanStoreDB() : SpanStoreDB = {
    SpanStoreDB(anormDB(), anormConnectionPoolSize())
  }

  def newAnormSpanStore( db : SpanStoreDB = newSpanStoreDB() ): SpanStore = {
    if (anormInstall()) {
      db.install()
    }
    new AnormSpanStore(db)
  }
}
