Output ../.context/vhs-demos/credential-masking/bruin-credential-masking.mp4

Require rg

Set Shell bash
Set FontSize 20
Set Width 1280
Set Height 720
Set Margin 0
Set Padding 28
Set LineHeight 1.15
Set TypingSpeed 30ms
Set Framerate 30
Set CursorBlink false
Set Theme "GitHub Dark"

Hide
Type 'repo_root="$(git rev-parse --show-toplevel)"; demo_root="$(mktemp -d "${TMPDIR:-/tmp}/bruin-vhs-credential.XXXXXX")"; cp -R "$repo_root/resources/credential-masking-demo/." "$demo_root"; mv "$demo_root/bruin.yml" "$demo_root/.bruin.yml"; git -C "$demo_root" init -q; cd "$demo_root"; unset NO_COLOR; export TERM=xterm-256color; export CLICOLOR=1; export PS1="$(tput setaf 6)\n❯$(tput sgr0) "; export BRUIN_CONFIG_FILE=.bruin.yml; bruin() { "$repo_root/bin/bruin-no-duckdb" "$@" --workers 1; }; shopt -s interactive_comments; clear'
Enter
Show
Type "# A demo token is stored as a generic Bruin secret"
Enter
Type "rg 'value:' .bruin.yml"
Enter
Sleep 2s

Type "bruin run pipeline --no-validation --no-timestamp --minimal-logs"
Enter
Sleep 8s
