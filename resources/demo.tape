# Where should we write the GIF?
Output demo.gif

# Set up a 1200x600 terminal with 46px font.
Set FontSize 12
Set Width 1600
Set Height 600
Set LineHeight 1
Set TypingSpeed 0.2
Set Framerate 60
Set PlaybackSpeed 4.0
Set CursorBlink false

Type "bruin init chess"
Enter
Sleep 5s

Type "cd chess"
Enter

Enter
Enter
Type "# let's first validate our pipeline
Enter
Enter
Enter
Sleep 6s

Type "bruin validate"
Enter
Sleep 6s
Enter
Enter
Type "# everything seems good, let's run the pipeline now
Enter
Type "# this will ingest Chess data into a DuckDB database and transform the data using SQL
Enter
Enter
Enter
Sleep 6s

Type "bruin run ."
Enter
Sleep 45s

Type "# let's check the resulting table in DuckDB
Enter
Enter
Enter

Type@100ms "duckdb duckdb.db 'select * from chess_playground.player_summary'"
Enter
Sleep 2s

Enter
Enter
Type "# that's it, from idea to full pipeline in seconds.
Enter
Sleep 10


