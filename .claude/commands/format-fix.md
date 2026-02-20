---
model: claude-sonnet-4-6
---

Run `make format` and automatically fix all formatting issues in the codebase.

Follow these steps:

1. First, run the format command to identify issues:
   ```
   make format
   ```

2. Check for any formatting or linting errors in the output.

3. If issues are detected, fix them automatically:
   - Import ordering issues: Reorganize imports according to Go conventions
   - Code formatting: Fix indentation, spacing, and line length issues
   - Style violations: Fix naming conventions and other style issues
   - Simple lint errors: Apply straightforward fixes

4. After making fixes, re-run `make format` to verify all issues are resolved.

5. If any issues require manual intervention:
   - Show the specific errors that couldn't be auto-fixed
   - Provide guidance on how to resolve them
   - Re-run validation after manual fixes

6. Show a summary of what was fixed and confirm the codebase passes formatting checks.
