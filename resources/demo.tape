# Where should we write the GIF?
Output demo.gif

# Set up a 1200x600 terminal with 46px font.
Set FontSize 20
Set Width 1200
Set Height 900
Set LineHeight 1.8
Set TypingSpeed 0.1
Set Framerate 60

Type "bruin validate examples/simple-pipeline"
Sleep 200ms
Enter
Sleep 5s

Type "bruin run examples/simple-pipeline/assets/hello_python.py"
Enter
Sleep 10s