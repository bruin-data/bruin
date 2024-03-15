""" @bruin
name: some-python-task
description: some description goes here
type: python
image: python:3.11
depends:
    - task1
    - task2
    - task3
    - task4
    - task5

parameters:
    param1: first-parameter
    param2: second-parameter
    param3: third-parameter

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
      query: select 5
      value: 16

secrets:
    - key: secret1
      inject_as: INJECTED_SECRET1
    - key: secret2

@bruin """

print('hello world')
