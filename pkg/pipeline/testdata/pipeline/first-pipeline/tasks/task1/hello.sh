uri: postgres://host:port/db
name: task1
type: bash
description: This is a hello world task
connection: conn1

depends:
  - gcs-to-bq

parameters:
  param1: value1
  param2: value2
