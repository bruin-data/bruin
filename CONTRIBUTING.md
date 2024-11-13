# Contributing to Bruin

First off, thank you for considering contributing to Bruin!

There are a few ways to contribute:
- [Reporting bugs](https://github.com/bruin-data/bruin/issues)
- Suggesting features
- Writing documentation
- Contributing code

We are pretty chill about the format of the contribution, although having an issue first to discuss the change is always a good idea. Feel free to deliver a PR early, we can always iterate on it.

## Writing Code
There are a few guidelines that you should follow when writing code:
- All new features should be covered by tests.
- All new features should be documented.
- The pull request should contain detailed description of the changes made, as well as the reasoning behind them.
- The code should be formatted and the linters should be passing: `make format`.

We use Makefile to build the code, which contains a set of commands to lint, test, build, and run the code.

### Installing dependencies

- Bruin requires Golang to be installed on your machine. You can install it [here](https://go.dev/doc/install).
- Once installed, you can install the dependencies by running `make deps`.

### Building & running the code

- To build the code, run `make build`.
- The resulting binary will be placed in the `bin` directory.

You can simply run it:
```sh
./bin/bruin --help
```

### Testing

You can run the tests by running `make test`.

