/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.streaming.ots

import com.alicloud.openservices.tablestore.SyncClient

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.{Offset, Source}


//class OTSSource(schema: StructType, sourceOptions: Map[String, String]) extends Source{
//  var client: SyncClient = _
//  initialize()
//  private def initialize(): Unit = {
////    val endPoint = sourceOptions.getOrElse("endpoint", {
////      throw new IllegalArgumentException("")
////    })
//    val endPoint = "http://testDB.cn-shanghai.ots.aliyuncs.com"
//    val accessKeyId = "LTAIbGcyxd7aM1CJ"
//    val accessKey = "Uk0vevvK02TYhPd16A62pOjgMr9hOA"
//    val instanceId = "testDB"
//
//    client = new SyncClient(endPoint, accessKeyId, accessKey, instanceId)
//  }
//
//  override def getOffset: Option[Offset] = ???
//
//  override def getBatch(start: Option[Offset], end: Offset): DataFrame = ???
//}
