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


object testInsert {
  def main(args: Array[String]): Unit = {
    val endPoint = "http://testDB.cn-shanghai.ots.aliyuncs.com"
    val accessKeyId = "LTAIbGcyxd7aM1CJ"
    val accessKey = "Uk0vevvK02TYhPd16A62pOjgMr9hOA"
    val instanceId = "testDB"
    val client : SyncClient = new SyncClient(endPoint, accessKeyId, accessKey, instanceId)
    val batchWriteRowRequest = new BatchWriteRowRequest()

    val primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder()
    primaryKeyBuilder.addPrimaryKeyColumn("id", PrimaryKeyValue.fromLong(1L))
    val primaryKey1 = primaryKeyBuilder.build()

    val rowPutChange1 = new RowPutChange("score", primaryKey1)
    rowPutChange1.addColumn(new Column("name", ColumnValue.fromString("Mike")))
    rowPutChange1.addColumn(new Column("score", ColumnValue.fromLong(98L)))

    batchWriteRowRequest.addRowChange(rowPutChange1)

    val primaryKeyBuilder2 = PrimaryKeyBuilder.createPrimaryKeyBuilder()
    primaryKeyBuilder2.addPrimaryKeyColumn("id", PrimaryKeyValue.fromLong(2L))
    val primaryKey2 = primaryKeyBuilder2.build()

    val rowPutChange2 = new RowPutChange("score", primaryKey2)
    rowPutChange2.addColumn(new Column("name", ColumnValue.fromString("George")))
    rowPutChange2.addColumn(new Column("score", ColumnValue.fromLong(34L)))

    batchWriteRowRequest.addRowChange(rowPutChange2)

    val batchWriteRowResponse = client.batchWriteRow(batchWriteRowRequest)

    // scalastyle:off
    println("isAll success:" + batchWriteRowResponse.isAllSucceed)
    // scalastyle:on

    if (!batchWriteRowResponse.isAllSucceed) {
      for (r <- batchWriteRowResponse.getFailedRows.asScala) {
        // scalastyle:off
        println("failure row:" +
          batchWriteRowRequest.getRowChange(r.getTableName, r.getIndex).getPrimaryKey)
        println("reason:" + r.getError)
        // scalastyle:on

      }
      val retryRequest =
        batchWriteRowRequest.createRequestForRetry(batchWriteRowResponse.getFailedRows)
    }

    client.shutdown()
  }
}
