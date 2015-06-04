/*
 * Copyright 2012 Twitter Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
import com.twitter.sbt.{BuildProperties,PackageDist,GitProject}
import sbt._
import com.twitter.scrooge.ScroogeSBT
import sbt.Keys._
import Keys._
import Tests._
import sbtassembly.Plugin._
import AssemblyKeys._
import com.typesafe.sbt.SbtSite.site
import com.typesafe.sbt.site.SphinxSupport.Sphinx

object Deploy extends Build {

  import Zipkin._

  lazy val deployedWeb = Project(
    id = "zipkin-deployment-web",
    base = file("zipkin-deployment-web"),
    settings = defaultSettings
  ).settings(
    libraryDependencies ++= Seq(
      finagle("zipkin"),
      finagle("stats"),
      anormDriverDependencies("mysql"),
      twitterServer
    )
  ).dependsOn(
    web, zipkinAggregate, anormDB, query, zookeeper
  )

  lazy val deployedCollector = Project(
    id = "zipkin-deployment-collector",
    base = file("zipkin-deployment-collector"),
    settings = defaultSettings
  ).settings(
    libraryDependencies ++= Seq(
      finagle("zipkin"),
      finagle("stats"),
      anormDriverDependencies("mysql"),
      twitterServer
    )
  ).dependsOn(
    sampler, collectorService, anormDB, receiverScribe, zookeeper
  )
}
