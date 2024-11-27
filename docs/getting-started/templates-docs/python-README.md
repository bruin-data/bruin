# Bruin - Python Template

This pipeline is a simple example of a Bruin pipeline. It demonstrates how to use the `bruin` CLI to build and run a pipeline.

The pipeline includes three Python assets that uses the same dependencies, but with 3 different versions of Python:
- 3.11
- 3.12
- 3.13

This pipeline is a good example of Bruin's Python execution abilities.

A couple of remarks:
- All of these assets are executed in isolated environments.
- All of the assets have separate `requirements.txt` files, which may contain different dependencies.

## Running the pipeline

Run the following command:
```shell
bruin run .
```

> [!NOTE]
> The first execution might take a bit longer since we'll need to install Python and the dependencies. The next executions should be super fast.

```shell
Starting the pipeline execution...

[2023-03-16T18:25:14Z] [worker-0] Running: dashboard.bruin-test
[2023-03-16T18:25:16Z] [worker-0] Completed: dashboard.bruin-test (1.681s)
[2023-03-16T18:25:16Z] [worker-4] Running: hello
[2023-03-16T18:25:16Z] [worker-4] [hello] >> Hello, world!
[2023-03-16T18:25:16Z] [worker-4] Completed: hello (116ms)

Executed 2 tasks in 1.798s
```

You can also run a single task:

```shell
bruin run assets/python311/asset.py                            
```

```shell
Starting the pipeline execution...

[2023-03-16T18:25:59Z] [worker-0] Running: hello
[2023-03-16T18:26:00Z] [worker-0] [hello] >> Hello, world!
[2023-03-16T18:26:00Z] [worker-0] Completed: hello (103ms)


Executed 1 tasks in 103ms
```

You can optionally pass a `--downstream` flag to run the task with all of its downstreams.

That's it, good luck!