# Lineage Panel

The Lineage Panel is located at the bottom of the VS Code interface, near the terminal tab. It provides a visual representation of the current asset's lineage.

## Functionality
- **Display Asset Lineage**
    - Shows how the current asset is connected to others in the data pipeline.
    - The view updates automatically when changes are made or when switching to another asset.

- **Expand Dependencies**: 
    - The lineage panel includes an options menu, collapsed by default. When expanded, you can choose which part of the lineage to display: `All (downstream, upstream)`, only one of them, or `Direct Only` (with `Direct Only` selected by default).
    - Each downstream or upstream node that has further dependencies displays a plus button. Clicking this button expands the node to show the dependencies in the same direction.

- **Navigate to Node Asset**  
    - When you click on a particular node, a link appears that allows you to navigate directly to the corresponding asset file by clicking it.

- **Control panel**: 
    - A control panel allows you to zoom in and out, fit the view, or lock the nodes in place to prevent displacement.

![Bruin Lineage Panel](../../public/vscode-extension/panels/lineage-panel/lineage-panel-with-options.gif)

- **Pipeline Lineage View** (New)  
    - A new **PipelineLineage** component has been added.
    - Navigation controls are now available in the expanded panel at the top right:
        - A **radio button** allows you to switch between `Asset View` and `Pipeline View`.
        - Selecting **Pipeline View** displays the data pipeline flow for the current asset.
        - In the **Pipeline View**, a button labeled **Asset View** appears in the top right. Clicking it switches back to showing the asset lineage for the current file.
    - This allows users to easily toggle between seeing an individual asset's lineage and its broader pipeline context.

![Bruin Lineage Panel](../../public/vscode-extension/panels/lineage-panel/pipeline-lineage-view.gif)


 
