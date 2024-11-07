# Contributing to Bruin

## Release

To create a new release, follow these steps:

1. Run the GitHub Action workflow `bump-version.yaml` to create a tag as a patch release.
2. Once the new tag is created, it will trigger the release workflow.
3. The release workflow will build the binary and publish it on GitHub as a pre-release.
4. The workflow will wait for GoReleaser to publish all artifacts.
5. After all artifacts are available, the release will be updated to the latest version.
6. Finally, the installer script will run on various devices.


