/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import sbt._
import Keys._

object Dependencies {

  val crossScalaVersionNumbers = Seq("2.11.8")
  val scalaVersionNumber = crossScalaVersionNumbers.last
  val akkaVersion = "2.5.13" // 升级 val akkaVersion = "2.4.16"
  val akkaHttpVersion = "10.1.3" // 升级 val akkaHttpVersion = "10.0.1"
  val hadoopVersion = "2.6.0"
  val hbaseVersion = "1.0.0"
  val commonsHttpVersion = "3.1"
  val commonsLoggingVersion = "1.1.3"
  val commonsLangVersion = "2.6"
  val commonsIOVersion = "2.4"
  val dataReplicationVersion = "0.7"
  val upickleVersion = "0.3.4"
  val junitVersion = "4.12"
  val kafkaVersion = "0.8.2.1"
  val kuduVersion = "1.7.0" // 新增
  val jsonSimpleVersion = "1.1"
  val storm09Version = "0.9.6"
  val stormVersion = "0.10.0"
  val slf4jVersion = "1.7.16"
  val guavaVersion = "16.0.1"
  val codahaleVersion = "3.0.2"
  val kryoVersion = "0.4.1"
  val gsCollectionsVersion = "6.2.0"
  val sprayVersion = "1.3.2"
  val sprayJsonVersion = "1.3.1"
  val scalaTestVersion = "2.2.0"
  val scalaCheckVersion = "1.11.3"
  val mockitoVersion = "1.10.17"
  val bijectionVersion = "0.8.0"
  val scalazVersion = "7.1.1"
  val algebirdVersion = "0.9.0"
  val chillVersion = "0.6.0"
  val jedisVersion = "2.9.0"
  val rabbitmqVersion = "3.5.3"
  val calciteVersion = "1.12.0"

  val annotationDependencies = Seq(
    // work around for compiler warnings like
    // "Class javax.annotation.CheckReturnValue not found - continuing with a stub"
    // see https://issues.scala-lang.org/browse/SI-8978
    // marked as "provided" to be excluded from assembling
    "com.google.code.findbugs" % "jsr305" % "3.0.2" % "provided"
  )

  val coreDependencies = Seq(
    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
      "commons-lang" % "commons-lang" % commonsLangVersion,
      /**
       * Overrides Netty version 3.10.3.Final used by Akka 2.4.2 to work-around netty hang issue
       * (https://github.com/gearpump/gearpump/issues/2020)
       *
       * Akka 2.4.2 by default use Netty 3.10.3.Final, which has a serious issue which can hang
       * the network. The same issue also happens in version range (3.10.0.Final, 3.10.5.Final)
       * Netty 3.10.6.Final have this issue fixed, however, we find there is a 20% performance
       * drop. So we decided to downgrade netty to 3.8.0.Final (Same version used in akka 2.3.12).
       *
       * @see https://github.com/gearpump/gearpump/pull/2017 for more discussions.
       */
      "io.netty" % "netty" % "3.8.0.Final",
      "com.typesafe.akka" %% "akka-remote" % akkaVersion
        exclude("io.netty", "netty"),

      "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
      "com.typesafe.akka" %% "akka-cluster-tools" % akkaVersion,
      "commons-logging" % "commons-logging" % commonsLoggingVersion,
      "com.typesafe.akka" %% "akka-distributed-data" % akkaVersion, // 升级 -experimental
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-agent" % akkaVersion,
      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
      // "com.typesafe.akka" %% "akka-kernel" % akkaVersion, // 去掉
      "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
      "org.scala-lang" % "scala-reflect" % scalaVersionNumber,
      "com.github.romix.akka" %% "akka-kryo-serialization" % kryoVersion,
      "com.google.guava" % "guava" % guavaVersion,
      "com.codahale.metrics" % "metrics-graphite" % codahaleVersion
        exclude("org.slf4j", "slf4j-api"),
      "com.codahale.metrics" % "metrics-jvm" % codahaleVersion
        exclude("org.slf4j", "slf4j-api"),

      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
      "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test",
      "org.mockito" % "mockito-core" % mockitoVersion % "test",
      "junit" % "junit" % junitVersion % "test"
    ) ++ annotationDependencies
  )
}