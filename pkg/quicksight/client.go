package quicksight

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/quicksight"
	qstypes "github.com/aws/aws-sdk-go-v2/service/quicksight/types"
)

type Client struct {
	config    Config
	awsClient *quicksight.Client
	accountID string
}

func NewClient(cfg Config) (*Client, error) {
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.AwsRegion),
	}
	if cfg.AwsAccessKeyID != "" && cfg.AwsSecretAccessKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AwsAccessKeyID, cfg.AwsSecretAccessKey, cfg.AwsSessionToken),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	return &Client{
		config:    cfg,
		awsClient: quicksight.NewFromConfig(awsCfg),
		accountID: cfg.AwsAccountID,
	}, nil
}

func (c *Client) Ping(ctx context.Context) error {
	_, err := c.awsClient.ListDashboards(ctx, &quicksight.ListDashboardsInput{
		AwsAccountId: aws.String(c.accountID),
		MaxResults:   aws.Int32(1),
	})
	if err != nil {
		return fmt.Errorf("failed to ping QuickSight: %w", err)
	}
	return nil
}

func (c *Client) GetName() string {
	return c.config.Name
}

// DataSetSummary holds summary info for a dataset as returned by ListDataSets.
type DataSetSummary struct {
	ID         string
	Name       string
	Arn        string
	ImportMode string
}

// DataSetDetail holds full detail for a dataset including columns and physical table maps.
type DataSetDetail struct {
	ID                string
	Name              string
	Arn               string
	ImportMode        string
	Columns           []DataSetColumn
	PhysicalTableMaps map[string]PhysicalTable
}

// DataSetColumn represents a column in a dataset.
type DataSetColumn struct {
	Name string
	Type string
}

// PhysicalTable represents a physical table map entry in a dataset.
type PhysicalTable struct {
	DatabaseName string
	SchemaName   string
	TableName    string
	Columns      []DataSetColumn
	DataSourceID string
}

// DashboardSummary holds summary info for a dashboard.
type DashboardSummary struct {
	ID   string
	Name string
	Arn  string
}

// DashboardDetail holds full detail for a dashboard including sheets and visuals.
type DashboardDetail struct {
	ID          string
	Name        string
	Arn         string
	DataSetArns []string
	Sheets      []SheetDetail
}

// SheetDetail represents a sheet within a dashboard.
type SheetDetail struct {
	SheetID string
	Name    string
	Visuals []VisualDetail
}

// VisualDetail represents a visual (chart) within a sheet.
type VisualDetail struct {
	Name       string
	Type       string
	DataSetID  string
	Dimensions []string
	Metrics    []string
}

// DataSourceSummary holds summary info for a data source.
type DataSourceSummary struct {
	ID   string
	Name string
	Type string
	Arn  string
}

// DataSourceDetail holds full detail for a data source connection.
type DataSourceDetail struct {
	ID             string
	Name           string
	Type           string
	Arn            string
	Database       string
	Schema         string
	Host           string
	Port           int32
	Catalog        string
	Warehouse      string
	ConnectionType string
}

func (c *Client) ListDataSets(ctx context.Context) ([]DataSetSummary, error) {
	var result []DataSetSummary
	var nextToken *string

	for {
		out, err := c.awsClient.ListDataSets(ctx, &quicksight.ListDataSetsInput{
			AwsAccountId: aws.String(c.accountID),
			NextToken:    nextToken,
			MaxResults:   aws.Int32(100),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to list datasets: %w", err)
		}

		for _, ds := range out.DataSetSummaries {
			result = append(result, DataSetSummary{
				ID:         aws.ToString(ds.DataSetId),
				Name:       aws.ToString(ds.Name),
				Arn:        aws.ToString(ds.Arn),
				ImportMode: string(ds.ImportMode),
			})
		}

		nextToken = out.NextToken
		if nextToken == nil {
			break
		}
	}

	return result, nil
}

func (c *Client) DescribeDataSet(ctx context.Context, dataSetID string) (*DataSetDetail, error) {
	out, err := c.awsClient.DescribeDataSet(ctx, &quicksight.DescribeDataSetInput{
		AwsAccountId: aws.String(c.accountID),
		DataSetId:    aws.String(dataSetID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to describe dataset '%s': %w", dataSetID, err)
	}

	ds := out.DataSet
	detail := &DataSetDetail{
		ID:         aws.ToString(ds.DataSetId),
		Name:       aws.ToString(ds.Name),
		Arn:        aws.ToString(ds.Arn),
		ImportMode: string(ds.ImportMode),
	}

	for _, col := range ds.OutputColumns {
		detail.Columns = append(detail.Columns, DataSetColumn{
			Name: aws.ToString(col.Name),
			Type: string(col.Type),
		})
	}

	detail.PhysicalTableMaps = make(map[string]PhysicalTable)
	for key, ptm := range ds.PhysicalTableMap {
		pt := extractPhysicalTable(ptm)
		detail.PhysicalTableMaps[key] = pt
	}

	return detail, nil
}

func extractPhysicalTable(ptm qstypes.PhysicalTable) PhysicalTable {
	pt := PhysicalTable{}

	switch v := ptm.(type) {
	case *qstypes.PhysicalTableMemberRelationalTable:
		pt.DatabaseName = aws.ToString(v.Value.Catalog)
		pt.SchemaName = aws.ToString(v.Value.Schema)
		pt.TableName = aws.ToString(v.Value.Name)
		pt.DataSourceID = aws.ToString(v.Value.DataSourceArn)
		for _, col := range v.Value.InputColumns {
			pt.Columns = append(pt.Columns, DataSetColumn{
				Name: aws.ToString(col.Name),
				Type: string(col.Type),
			})
		}
	case *qstypes.PhysicalTableMemberCustomSql:
		pt.DataSourceID = aws.ToString(v.Value.DataSourceArn)
		for _, col := range v.Value.Columns {
			pt.Columns = append(pt.Columns, DataSetColumn{
				Name: aws.ToString(col.Name),
				Type: string(col.Type),
			})
		}
	case *qstypes.PhysicalTableMemberS3Source:
		pt.DataSourceID = aws.ToString(v.Value.DataSourceArn)
		for _, col := range v.Value.InputColumns {
			pt.Columns = append(pt.Columns, DataSetColumn{
				Name: aws.ToString(col.Name),
				Type: string(col.Type),
			})
		}
	}

	return pt
}

func (c *Client) ListDashboards(ctx context.Context) ([]DashboardSummary, error) {
	var result []DashboardSummary
	var nextToken *string

	for {
		out, err := c.awsClient.ListDashboards(ctx, &quicksight.ListDashboardsInput{
			AwsAccountId: aws.String(c.accountID),
			NextToken:    nextToken,
			MaxResults:   aws.Int32(100),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to list dashboards: %w", err)
		}

		for _, d := range out.DashboardSummaryList {
			result = append(result, DashboardSummary{
				ID:   aws.ToString(d.DashboardId),
				Name: aws.ToString(d.Name),
				Arn:  aws.ToString(d.Arn),
			})
		}

		nextToken = out.NextToken
		if nextToken == nil {
			break
		}
	}

	return result, nil
}

func (c *Client) DescribeDashboard(ctx context.Context, dashboardID string) (*DashboardDetail, error) {
	out, err := c.awsClient.DescribeDashboard(ctx, &quicksight.DescribeDashboardInput{
		AwsAccountId: aws.String(c.accountID),
		DashboardId:  aws.String(dashboardID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to describe dashboard '%s': %w", dashboardID, err)
	}

	d := out.Dashboard
	detail := &DashboardDetail{
		ID:   aws.ToString(d.DashboardId),
		Name: aws.ToString(d.Name),
		Arn:  aws.ToString(d.Arn),
	}

	if d.Version != nil {
		detail.DataSetArns = append(detail.DataSetArns, d.Version.DataSetArns...)
	}

	return detail, nil
}

func (c *Client) DescribeDashboardDefinition(ctx context.Context, dashboardID string) (*DashboardDetail, error) {
	out, err := c.awsClient.DescribeDashboardDefinition(ctx, &quicksight.DescribeDashboardDefinitionInput{
		AwsAccountId: aws.String(c.accountID),
		DashboardId:  aws.String(dashboardID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to describe dashboard definition '%s': %w", dashboardID, err)
	}

	detail := &DashboardDetail{
		ID:   dashboardID,
		Name: aws.ToString(out.Name),
	}

	if out.Definition != nil {
		for _, dsID := range out.Definition.DataSetIdentifierDeclarations {
			detail.DataSetArns = append(detail.DataSetArns, aws.ToString(dsID.DataSetArn))
		}

		for _, sheet := range out.Definition.Sheets {
			sd := SheetDetail{
				SheetID: aws.ToString(sheet.SheetId),
				Name:    aws.ToString(sheet.Name),
			}
			for _, visual := range sheet.Visuals {
				vd := extractVisualDetail(visual)
				if vd.Name != "" || vd.Type != "" {
					sd.Visuals = append(sd.Visuals, vd)
				}
			}
			detail.Sheets = append(detail.Sheets, sd)
		}
	}

	return detail, nil
}

func safeVisualTitle(title *qstypes.VisualTitleLabelOptions) string {
	if title == nil || title.FormatText == nil {
		return ""
	}
	return aws.ToString(title.FormatText.PlainText)
}

func extractVisualDetail(v qstypes.Visual) VisualDetail { //nolint:cyclop
	vd := VisualDetail{}

	switch {
	case v.BarChartVisual != nil:
		vd.Name = safeVisualTitle(v.BarChartVisual.Title)
		vd.Type = "BarChart"
		if cc := v.BarChartVisual.ChartConfiguration; cc != nil {
			vd.Dimensions, vd.Metrics = extractFieldWells(cc.FieldWells)
		}
	case v.LineChartVisual != nil:
		vd.Name = safeVisualTitle(v.LineChartVisual.Title)
		vd.Type = "LineChart"
		if cc := v.LineChartVisual.ChartConfiguration; cc != nil {
			vd.Dimensions, vd.Metrics = extractLineChartFieldWells(cc.FieldWells)
		}
	case v.PieChartVisual != nil:
		vd.Name = safeVisualTitle(v.PieChartVisual.Title)
		vd.Type = "PieChart"
		if cc := v.PieChartVisual.ChartConfiguration; cc != nil {
			vd.Dimensions, vd.Metrics = extractPieChartFieldWells(cc.FieldWells)
		}
	case v.TableVisual != nil:
		vd.Name = safeVisualTitle(v.TableVisual.Title)
		vd.Type = "Table"
		if cc := v.TableVisual.ChartConfiguration; cc != nil {
			vd.Dimensions, vd.Metrics = extractTableFieldWells(cc.FieldWells)
		}
	case v.PivotTableVisual != nil:
		vd.Name = safeVisualTitle(v.PivotTableVisual.Title)
		vd.Type = "PivotTable"
		if cc := v.PivotTableVisual.ChartConfiguration; cc != nil {
			vd.Dimensions, vd.Metrics = extractPivotTableFieldWells(cc.FieldWells)
		}
	case v.KPIVisual != nil:
		vd.Name = safeVisualTitle(v.KPIVisual.Title)
		vd.Type = "KPI"
		if cc := v.KPIVisual.ChartConfiguration; cc != nil {
			vd.Dimensions, vd.Metrics = extractKPIFieldWells(cc.FieldWells)
		}
	case v.GaugeChartVisual != nil:
		vd.Name = safeVisualTitle(v.GaugeChartVisual.Title)
		vd.Type = "GaugeChart"
	case v.ScatterPlotVisual != nil:
		vd.Name = safeVisualTitle(v.ScatterPlotVisual.Title)
		vd.Type = "ScatterPlot"
	case v.FunnelChartVisual != nil:
		vd.Name = safeVisualTitle(v.FunnelChartVisual.Title)
		vd.Type = "FunnelChart"
	case v.HeatMapVisual != nil:
		vd.Name = safeVisualTitle(v.HeatMapVisual.Title)
		vd.Type = "HeatMap"
	case v.TreeMapVisual != nil:
		vd.Name = safeVisualTitle(v.TreeMapVisual.Title)
		vd.Type = "TreeMap"
	case v.ComboChartVisual != nil:
		vd.Name = safeVisualTitle(v.ComboChartVisual.Title)
		vd.Type = "ComboChart"
	case v.WordCloudVisual != nil:
		vd.Name = safeVisualTitle(v.WordCloudVisual.Title)
		vd.Type = "WordCloud"
	case v.SankeyDiagramVisual != nil:
		vd.Name = safeVisualTitle(v.SankeyDiagramVisual.Title)
		vd.Type = "SankeyDiagram"
	case v.WaterfallVisual != nil:
		vd.Name = safeVisualTitle(v.WaterfallVisual.Title)
		vd.Type = "Waterfall"
	case v.BoxPlotVisual != nil:
		vd.Name = safeVisualTitle(v.BoxPlotVisual.Title)
		vd.Type = "BoxPlot"
	case v.HistogramVisual != nil:
		vd.Name = safeVisualTitle(v.HistogramVisual.Title)
		vd.Type = "Histogram"
	case v.InsightVisual != nil:
		vd.Name = safeVisualTitle(v.InsightVisual.Title)
		vd.Type = "Insight"
	}

	return vd
}

func extractFieldWells(fw *qstypes.BarChartFieldWells) (dims []string, metrics []string) {
	if fw == nil || fw.BarChartAggregatedFieldWells == nil {
		return nil, nil
	}
	agg := fw.BarChartAggregatedFieldWells
	for _, d := range agg.Category {
		dims = append(dims, extractDimensionFieldName(d))
	}
	for _, m := range agg.Values {
		metrics = append(metrics, extractMeasureFieldName(m))
	}
	return dims, metrics
}

func extractLineChartFieldWells(fw *qstypes.LineChartFieldWells) (dims []string, metrics []string) {
	if fw == nil || fw.LineChartAggregatedFieldWells == nil {
		return nil, nil
	}
	agg := fw.LineChartAggregatedFieldWells
	for _, d := range agg.Category {
		dims = append(dims, extractDimensionFieldName(d))
	}
	for _, m := range agg.Values {
		metrics = append(metrics, extractMeasureFieldName(m))
	}
	return dims, metrics
}

func extractPieChartFieldWells(fw *qstypes.PieChartFieldWells) (dims []string, metrics []string) {
	if fw == nil || fw.PieChartAggregatedFieldWells == nil {
		return nil, nil
	}
	agg := fw.PieChartAggregatedFieldWells
	for _, d := range agg.Category {
		dims = append(dims, extractDimensionFieldName(d))
	}
	for _, m := range agg.Values {
		metrics = append(metrics, extractMeasureFieldName(m))
	}
	return dims, metrics
}

func extractTableFieldWells(fw *qstypes.TableFieldWells) (dims []string, metrics []string) {
	if fw == nil || fw.TableAggregatedFieldWells == nil {
		return nil, nil
	}
	agg := fw.TableAggregatedFieldWells
	for _, d := range agg.GroupBy {
		dims = append(dims, extractDimensionFieldName(d))
	}
	for _, m := range agg.Values {
		metrics = append(metrics, extractMeasureFieldName(m))
	}
	return dims, metrics
}

func extractPivotTableFieldWells(fw *qstypes.PivotTableFieldWells) (dims []string, metrics []string) {
	if fw == nil || fw.PivotTableAggregatedFieldWells == nil {
		return nil, nil
	}
	agg := fw.PivotTableAggregatedFieldWells
	for _, d := range agg.Rows {
		dims = append(dims, extractDimensionFieldName(d))
	}
	for _, d := range agg.Columns {
		dims = append(dims, extractDimensionFieldName(d))
	}
	for _, m := range agg.Values {
		metrics = append(metrics, extractMeasureFieldName(m))
	}
	return dims, metrics
}

func extractKPIFieldWells(fw *qstypes.KPIFieldWells) (dims []string, metrics []string) {
	if fw == nil {
		return nil, nil
	}
	for _, d := range fw.TrendGroups {
		dims = append(dims, extractDimensionFieldName(d))
	}
	for _, m := range fw.Values {
		metrics = append(metrics, extractMeasureFieldName(m))
	}
	for _, m := range fw.TargetValues {
		metrics = append(metrics, extractMeasureFieldName(m))
	}
	return dims, metrics
}

func extractDimensionFieldName(d qstypes.DimensionField) string {
	switch {
	case d.CategoricalDimensionField != nil:
		return aws.ToString(d.CategoricalDimensionField.Column.ColumnName)
	case d.DateDimensionField != nil:
		return aws.ToString(d.DateDimensionField.Column.ColumnName)
	case d.NumericalDimensionField != nil:
		return aws.ToString(d.NumericalDimensionField.Column.ColumnName)
	}
	return ""
}

func extractMeasureFieldName(m qstypes.MeasureField) string {
	switch {
	case m.NumericalMeasureField != nil:
		return aws.ToString(m.NumericalMeasureField.Column.ColumnName)
	case m.CategoricalMeasureField != nil:
		return aws.ToString(m.CategoricalMeasureField.Column.ColumnName)
	case m.DateMeasureField != nil:
		return aws.ToString(m.DateMeasureField.Column.ColumnName)
	case m.CalculatedMeasureField != nil:
		return aws.ToString(m.CalculatedMeasureField.FieldId)
	}
	return ""
}

func (c *Client) ListDataSources(ctx context.Context) ([]DataSourceSummary, error) {
	var result []DataSourceSummary
	var nextToken *string

	for {
		out, err := c.awsClient.ListDataSources(ctx, &quicksight.ListDataSourcesInput{
			AwsAccountId: aws.String(c.accountID),
			NextToken:    nextToken,
			MaxResults:   aws.Int32(100),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to list data sources: %w", err)
		}

		for _, ds := range out.DataSources {
			result = append(result, DataSourceSummary{
				ID:   aws.ToString(ds.DataSourceId),
				Name: aws.ToString(ds.Name),
				Type: string(ds.Type),
				Arn:  aws.ToString(ds.Arn),
			})
		}

		nextToken = out.NextToken
		if nextToken == nil {
			break
		}
	}

	return result, nil
}

func (c *Client) DescribeDataSource(ctx context.Context, dataSourceID string) (*DataSourceDetail, error) {
	out, err := c.awsClient.DescribeDataSource(ctx, &quicksight.DescribeDataSourceInput{
		AwsAccountId: aws.String(c.accountID),
		DataSourceId: aws.String(dataSourceID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to describe data source '%s': %w", dataSourceID, err)
	}

	ds := out.DataSource
	detail := &DataSourceDetail{
		ID:   aws.ToString(ds.DataSourceId),
		Name: aws.ToString(ds.Name),
		Type: string(ds.Type),
		Arn:  aws.ToString(ds.Arn),
	}

	if ds.DataSourceParameters != nil {
		extractDataSourceParameters(ds.DataSourceParameters, detail)
	}

	return detail, nil
}

func extractDataSourceParameters(params qstypes.DataSourceParameters, detail *DataSourceDetail) { //nolint:cyclop
	switch v := params.(type) {
	case *qstypes.DataSourceParametersMemberRdsParameters:
		detail.Database = aws.ToString(v.Value.Database)
		detail.Host = aws.ToString(v.Value.InstanceId)
		detail.ConnectionType = "rds"
	case *qstypes.DataSourceParametersMemberRedshiftParameters:
		detail.Database = aws.ToString(v.Value.Database)
		detail.Host = aws.ToString(v.Value.Host)
		detail.Port = v.Value.Port
		detail.Schema = aws.ToString(v.Value.ClusterId)
		detail.ConnectionType = "redshift"
	case *qstypes.DataSourceParametersMemberPostgreSqlParameters:
		detail.Database = aws.ToString(v.Value.Database)
		detail.Host = aws.ToString(v.Value.Host)
		detail.Port = aws.ToInt32(v.Value.Port)
		detail.ConnectionType = "postgres"
	case *qstypes.DataSourceParametersMemberMySqlParameters:
		detail.Database = aws.ToString(v.Value.Database)
		detail.Host = aws.ToString(v.Value.Host)
		detail.Port = aws.ToInt32(v.Value.Port)
		detail.ConnectionType = "mysql"
	case *qstypes.DataSourceParametersMemberAuroraParameters:
		detail.Database = aws.ToString(v.Value.Database)
		detail.Host = aws.ToString(v.Value.Host)
		detail.Port = aws.ToInt32(v.Value.Port)
		detail.ConnectionType = "aurora"
	case *qstypes.DataSourceParametersMemberAuroraPostgreSqlParameters:
		detail.Database = aws.ToString(v.Value.Database)
		detail.Host = aws.ToString(v.Value.Host)
		detail.Port = aws.ToInt32(v.Value.Port)
		detail.ConnectionType = "aurora_postgres"
	case *qstypes.DataSourceParametersMemberSnowflakeParameters:
		detail.Database = aws.ToString(v.Value.Database)
		detail.Host = aws.ToString(v.Value.Host)
		detail.Warehouse = aws.ToString(v.Value.Warehouse)
		detail.ConnectionType = "snowflake"
	case *qstypes.DataSourceParametersMemberAthenaParameters:
		detail.ConnectionType = "athena"
	case *qstypes.DataSourceParametersMemberS3Parameters:
		detail.ConnectionType = "s3"
	case *qstypes.DataSourceParametersMemberDatabricksParameters:
		detail.Host = aws.ToString(v.Value.Host)
		detail.Port = aws.ToInt32(v.Value.Port)
		detail.ConnectionType = "databricks"
	}
}

func (c *Client) CreateIngestion(ctx context.Context, dataSetID, ingestionID string) error {
	_, err := c.awsClient.CreateIngestion(ctx, &quicksight.CreateIngestionInput{
		AwsAccountId: aws.String(c.accountID),
		DataSetId:    aws.String(dataSetID),
		IngestionId:  aws.String(ingestionID),
	})
	if err != nil {
		return fmt.Errorf("failed to create ingestion for dataset '%s': %w", dataSetID, err)
	}
	return nil
}
