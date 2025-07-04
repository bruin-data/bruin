# Dashboard Assets

Dashboard assets serve as placeholders to represent where data is collected, queried, or visualized by any dashboarding tools in your pipeline.

They do not perform any computation, transformation, or data movement of their own, but instead are used to preserve an accurate overview of your data pipeline’s lineage.

This helps you more accurately visualize and document the tools used in your data pipeline.

Bruin supports the following dashboard tools as assets:

Amazon QuickSight, Apache Superset, Domo, Good Data, Grafana, Looker, Metabase, Mode BI, Power BI, Qlik Sense, Qlik View, Redash, Sisense, and Tableau.

## Definition Schema
Dashboard assets are defined using the extension `{asset_name}.asset.yml`. Here is an example of the schema:

```yaml
name: dashboard.quicksight
type: quicksight
description: "Dashboard sales data visualization."

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
name: myschema.asset_name
type: quicksight
```

### Apache Superset
```yaml
name: myschema.asset_name
type: superset
```

### Domo
```yaml
name: myschema.asset_name
type: domo
```

### Good Data
```yaml
name: myschema.asset_name
type: gooddata
```

### Grafana
```yaml
name: myschema.asset_name
type: grafana
```

### Looker
```yaml
name: myschema.asset_name
type: looker
```

### Metabase
```yaml
name: myschema.asset_name
type: metabase
```

### Mode BI
```yaml
name: myschema.asset_name
type: modebi
```

### Power BI
```yaml
name: myschema.asset_name
type: powerbi
```

### Qlik Sense
```yaml
name: myschema.asset_name
type: qliksense
```

### Qlik View
```yaml
name: myschema.asset_name
type: qlikview
```

### Redash
```yaml
name: myschema.asset_name
type: redash
```

### Sisense
```yaml
name: myschema.asset_name
type: sisense
```

### Tableau
```yaml
name: myschema.asset_name
type: tableau
```



