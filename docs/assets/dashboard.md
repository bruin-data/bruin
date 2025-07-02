# Dashboard Assets

Dashboard assets in Bruin are special, lightweight assets that serve as placeholders or organizational markers within your pipeline. They do not perform any computation, transformation, or data movement. Instead, they are used to represent documentation points in your data workflows and lineages.

## Purpose
- **Placeholders:** Indicate where processes such as data visualizations, progress tracking or communication with clients takes place.
- **Organization:** Help structure your pipeline lineage visually, making it easier to understand and maintain.
- **Documentation:** Provide context or notes within your pipeline without affecting execution.


## Definition
Dashboard assets are defined using the extension `{asset_name}.asset.yml`. They have minimal a configuration and only specify a `name` and `type`.

## Examples

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



