# Dashboard Assets

Dashboard assets serve as placeholders to represent where data is collected, queried, or visualized by any dashboarding tools in your pipeline.

They do not perform any computation, transformation, or data movement of their own, but instead are used to preserve an accurate overview of your data pipeline’s lineage.

This helps you visualize and document the tools used in your data pipeline, making it easier to understand and maintain.

We support a variety of dashboard tools, including:
Amazon QuickSight, Apache Superset, Domo, Good Data, Grafana, Looker, Metabase, Mode BI, Power BI, Qlik Sense, Qlik View, Redash, Sisense, and Tableau.

## Definition Schema
Dashboard assets are defined using the extension `{asset_name}.asset.yml`. Here is an example of the schema:

```yaml
name: dashboard.quicksight
type: quicksight

depends:
    - schema.my_asset

owner: my-team@acme-corp.com

tags:
  - dashboard
  - team.xyz
```

## Supported Dashboard Tools

### Amazon QuickSight
```yaml
name: dashboard.quicksight
type: quicksight
```

### Apache Superset
```yaml
name: dashboard.superset
type: superset
```

### Domo
```yaml
name: dashboard.domo
type: domo
```

### Good Data
```yaml
name: dashboard.gooddata
type: gooddata
```

### Grafana
```yaml
name: dashboard.grafana
type: grafana
```

### Looker
```yaml
name: dashboard.looker
type: looker
```

### Metabase
```yaml
name: dashboard.metabase
type: metabase
```

### Mode BI
```yaml
name: dashboard.modebi
type: modebi
```

### Power BI
```yaml
name: dashboard.powerbi
type: powerbi
```

### Qlik Sense
```yaml
name: dashboard.qliksense
type: qliksense
```

### Qlik View
```yaml
name: dashboard.qlikview
type: qlikview
```

### Redash
```yaml
name: dashboard.redash
type: redash
```

### Sisense
```yaml
name: dashboard.sisense
type: sisense
```

### Tableau
```yaml
name: dashboard.tableau 
type: tableau
```



