# @bruin
# name: some-r-task
# description: R task with multiline configuration
# type: r
# image: r:4.3
# instance: b1.nano
# depends:
#     - task1
#     - task2
#     - task3
#
# parameters:
#     param1: first-parameter
#     param2: second-parameter
#     param3: third-parameter
#
# secrets:
#     - key: secret1
#       inject_as: INJECTED_SECRET1
#     - key: secret2
# @bruin

cat("Hello from R!\n")
print("This is an R script")
