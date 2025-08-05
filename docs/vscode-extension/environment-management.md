# Environment Management

The Environment Management UI allows you to create, update, and delete environments directly from the VS Code extension, providing a seamless way to manage your deployment targets and configurations.

## Overview

Environments in Bruin represent different deployment targets (e.g., development, staging, production) with their own configurations, connections, and settings. The Environment Management UI streamlines the process of managing these environments without requiring manual file editing.

## Accessing Environment Management

Environment management features are integrated into the extension's interface and can be accessed through:
- The Settings tab in the Side Panel
- Environment dropdown selections throughout the extension
- Connection management interfaces

## Features

### Create New Environments
- **Interactive Creation**: Use the UI to create new environments with guided input forms
- **Configuration Templates**: Start with pre-configured templates or create custom environments from scratch
- **Validation**: Built-in validation ensures environment configurations are correct before creation

### Update Existing Environments
- **In-Place Editing**: Modify environment settings directly through the UI
- **Real-Time Validation**: Changes are validated as you type to prevent configuration errors
- **Immediate Application**: Updates are applied immediately without requiring restarts

### Delete Environments
- **Safe Deletion**: Delete environments with confirmation prompts to prevent accidental removal
- **Dependency Checking**: Warns if environments are referenced by assets or connections before deletion
- **Clean Removal**: Completely removes environment configurations and associated references

### Enhanced Environment UI
- **Visual Management**: Clear visual interface showing all available environments
- **Status Indicators**: Visual indicators show environment health and connectivity status
- **Quick Actions**: One-click actions for common environment operations

## How to Use

### Creating a New Environment

1. **Access Settings**: Navigate to the Settings tab in the Side Panel
2. **Environment Section**: Find the environment management section
3. **Create Environment**: Click the "Create Environment" button
4. **Fill Details**: Enter environment name, description, and configuration details
5. **Validate**: The system will validate your inputs automatically
6. **Save**: Click "Save" to create the new environment

### Updating an Environment

1. **Select Environment**: Choose the environment you want to modify from the dropdown
2. **Edit Mode**: Click the edit button next to the environment name
3. **Modify Settings**: Update the environment configuration as needed
4. **Apply Changes**: Save your changes to update the environment

### Deleting an Environment

1. **Select Environment**: Choose the environment to delete
2. **Delete Action**: Click the delete button (trash icon)
3. **Confirm Deletion**: Confirm the deletion in the popup dialog
4. **Dependencies Check**: Review any warnings about dependent assets or connections
5. **Complete Deletion**: Confirm to permanently remove the environment

## Benefits

- **Streamlined Workflow**: Manage environments without leaving VS Code
- **Error Prevention**: Built-in validation prevents configuration mistakes
- **Visual Feedback**: Clear UI indicators show environment status and health
- **Safe Operations**: Confirmation dialogs and dependency checks prevent accidental data loss
- **Integration**: Seamlessly works with existing Bruin assets and connections

![Environment Management UI](../public/vscode-extension/environment-management/manage-environments.gif)