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

import scala.collection.JavaConverters._

import com.alicloud.openservices.tablestore.SyncClient
import com.alicloud.openservices.tablestore.model._


object testStream {
  def main(args: Array[String]): Unit = {
    val endPoint = "http://testDB.cn-shanghai.ots.aliyuncs.com"
    val accessKeyId = "LTAIbGcyxd7aM1CJ"
    val accessKey = "Uk0vevvK02TYhPd16A62pOjgMr9hOA"
    val instanceId = "testDB"
    val client : SyncClient = new SyncClient(endPoint, accessKeyId, accessKey, instanceId)

    val listStreamRequest = new ListStreamRequest("score")
    val result = client.listStream(listStreamRequest)
    val streams = result.getStreams.asScala
    val streamId = streams.headOption.map(_.getStreamId).orNull
    if(streamId != null) {
      val desRequest = new DescribeStreamRequest(streamId)
      val response = client.describeStream(desRequest)
      val firstShardId =
        response.getShards.asScala.headOption.map(_.getShardId).orNull

      if(firstShardId != null) {
        val getShardIterRequest = new GetShardIteratorRequest(streamId, firstShardId)
        val shardIterResponse = client.getShardIterator(getShardIterRequest)
        val sIter = shardIterResponse.getShardIterator

        val streamRecordRequest = new GetStreamRecordRequest(sIter)
        // scalastyle:off
        println(streamRecordRequest.getLimit)
        streamRecordRequest.setLimit(7)
        println(streamRecordRequest.getLimit)
        val streamRecordResponse = client.getStreamRecord(streamRecordRequest)
        println("next Iter:" + streamRecordResponse.getNextShardIterator)
        val records = streamRecordResponse.getRecords.asScala

        println("curr Iter:" + sIter)
        println("next Iter:" + streamRecordResponse.getNextShardIterator)
        println("records size:" + records.size)
        records.foreach(println)

        println("next Iter:" + streamRecordResponse.getNextShardIterator)
        println("curr Iter:"+ shardIterResponse.getShardIterator)


        val nextIter = streamRecordResponse.getNextShardIterator
        val streamRecordRequest2 = new GetStreamRecordRequest(nextIter)
        println(streamRecordRequest2.getLimit)
        streamRecordRequest2.setLimit(7)
        println(streamRecordRequest2.getLimit)
        val streamRecordResponse2 = client.getStreamRecord(streamRecordRequest2)
        println("next Iter:" + streamRecordResponse2.getNextShardIterator)
        val records2 = streamRecordResponse2.getRecords.asScala

        println("records2 size:" + records2.size)
        records2.foreach(println)

        // scalastyle:on

      }

    }


    client.shutdown()
  }
}