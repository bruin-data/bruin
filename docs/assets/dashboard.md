# Dashboard Assets

Dashboard assets represent where your reports and dashboards within your data pipelines. They are used to preserve an accurate overview of your data pipeline’s lineage.

Bruin supports the following dashboard tools as assets:

- Amazon QuickSight: `quicksight`
- Apache Superset: `superset`
- Domo: `domo`
- Good Data: `gooddata`
- Grafana: `grafana`
- Looker: `looker`
- Looker Studio: `looker_studio`
- Metabase: `metabase`
- Mode BI: `modebi`
- Power BI: `powerbi`
- Qlik Sense: `qliksense`
- Qlik View: `qlikview`
- Redash: `redash`
- Sisense: `sisense`
- Tableau: `tableau`

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

### Looker Studio
```yaml
name: myschema.asset_name
type: looker_studio
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
Tableau assets allow you to both define and refresh Tableau dashboards, workbooks, and worksheets. Please see the [Tableau assets](./tableau-refresh) for more information.

```yaml
name: myschema.asset_name
type: tableau
```



