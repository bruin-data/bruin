"""@bruin

name: some-python-task
description: some description goes here
tags:
  - tag1
  - tag2:value2

depends:
  - task1
  - task2
  - task3
  - task4
  - task5
image: python:3.11
instance: b1.nano
owner: jane.doe@getbruin.com

parameters:
  param1: first-parameter
  param2: second-parameter
  param3: third-parameter

secrets:
  - key: secret1
    inject_as: INJECTED_SECRET1
  - key: secret2
    inject_as: secret2

columns:
  - name: col1
    type: string
    checks:
      - name: not_null
      - name: positive
      - name: unique
  - name: col2
    type: string
    checks:
      - name: not_null
      - name: unique

custom_checks:
  - name: check1
    description: test description
    value: 16
    blocking: false
    query: select 5

@bruin"""

print('hello world')
