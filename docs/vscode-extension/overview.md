# Bruin VSCode Extension

The Bruin VSCode extension complements the Bruin CLI by offering a more visual and interactive approach to managing data pipelines. Integrated directly into VSCode, it simplifies tasks like building, managing, and deploying pipelines, making it easier for developers to interact with their projects. This extension is packed with features that enhance productivity and streamline the workflow.

### What is a Bruin Section?
A **Bruin section** refers to a block of code within SQL or Python files that is specifically designated for Bruin-related functionality. These sections are typically enclosed within specific delimiters, allowing the extension to identify and manage them effectively. Users can fold or expand these sections to improve code readability and organization.

#### Example of a Bruin Section in SQL
```sql
/* @bruin
  This is a Bruin section in SQL.
  It can contain Bruin-specific commands or configurations.
@bruin */
SELECT * FROM users;
```
### Example of a Bruin Section in Python
```python
"""
@bruin
This is a Bruin section in Python.
It can include Bruin-related logic or configurations.
@bruin
"""
def fetch_users():
    pass
```
## Key Features:
- **Syntax Coloring, Autocompletion, and Snippets**: Benefit from enhanced coding support with syntax highlighting, autocompletion, and pre-built snippets.
- **Bruin CLI Integration**: Execute Bruin CLI commands through intuitive UI elements, such as buttons and panels, for a smoother workflow.
- **Asset Lineage Visualization**: Visualize the relationships between data assets for better understanding and tracking.
- **Real-time Feedback**: Receive updates and error messages in VSCode, improving the overall user experience.

