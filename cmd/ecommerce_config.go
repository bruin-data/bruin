package cmd

import (
	"fmt"
	"strings"
)

// generateBruinYML generates the .bruin.yml content based on the user's ecommerce stack choices.
func generateBruinYML(c *EcommerceChoices) string {
	var connections []string

	// Warehouse connection
	switch c.Warehouse {
	case warehouseClickHouse:
		connections = append(connections, `      clickhouse:
        - name: "clickhouse-default"
          host: "your-host.clickhouse.cloud"
          port: 9440
          username: "default"
          password: "your-password"
          database: "default"
          ssl_mode: "true"`)
	case warehouseBigQuery:
		connections = append(connections, `      google_cloud_platform:
        - name: "bigquery-default"
          project_id: "your-gcp-project-id"
          service_account_file: "/path/to/your-service-account.json"`)
	case warehouseSnowflake:
		connections = append(connections, `      snowflake:
        - name: "snowflake-default"
          account: "your-account-id"
          username: "your-username"
          password: "your-password"
          database: "your-database"
          warehouse: "your-warehouse"
          schema: "PUBLIC"`)
	}

	// Shopify connection (always)
	connections = append(connections, `      shopify:
        - name: "shopify"
          api_key: "your-shopify-admin-api-key"
          url: "your-store.myshopify.com"`)

	// Payments connection
	if c.Payments == paymentsStripe {
		connections = append(connections, `      stripe:
        - name: "stripe"
          api_key: "sk_live_your-stripe-secret-key"`)
	}

	// Marketing connection
	switch c.Marketing {
	case marketingKlaviyo:
		connections = append(connections, `      klaviyo:
        - name: "klaviyo"
          api_key: "your-klaviyo-api-key"`)
	case marketingHubSpot:
		connections = append(connections, `      hubspot:
        - name: "hubspot"
          api_key: "your-hubspot-api-key"`)
	}

	// Ads connections
	for _, ad := range c.Ads {
		switch ad {
		case adsFacebook:
			connections = append(connections, `      facebook_ads:
        - name: "facebook_ads"
          access_token: "your-facebook-access-token"
          account_id: "your-ad-account-id"`)
		case adsGoogle:
			connections = append(connections, `      google_ads:
        - name: "google_ads"
          developer_token: "your-developer-token"
          client_id: "your-client-id"
          client_secret: "your-client-secret"
          refresh_token: "your-refresh-token"
          customer_id: "your-customer-id"`)
		case adsTikTok:
			connections = append(connections, `      tiktok_ads:
        - name: "tiktok_ads"
          access_token: "your-tiktok-access-token"
          advertiser_id: "your-advertiser-id"`)
		}
	}

	// Analytics connection
	switch c.Analytics {
	case analyticsGA4:
		connections = append(connections, `      google_analytics:
        - name: "google_analytics"
          service_account_file: "/path/to/your-ga4-service-account.json"
          property_id: "your-ga4-property-id"`)
	case analyticsMixpanel:
		connections = append(connections, `      mixpanel:
        - name: "mixpanel"
          api_secret: "your-mixpanel-api-secret"
          project_id: "your-mixpanel-project-id"`)
	}

	return fmt.Sprintf(`default_environment: default

environments:
  default:
    connections:
%s
`, strings.Join(connections, "\n"))
}

// printEcommerceSummary prints a summary of what was generated.
func printEcommerceSummary(c *EcommerceChoices) {
	infoPrinter.Println("\n  Stack summary:")
	infoPrinter.Printf("    Warehouse:  %s\n", c.Warehouse)
	infoPrinter.Printf("    Payments:   %s\n", c.Payments)
	infoPrinter.Printf("    Marketing:  %s\n", c.Marketing)
	infoPrinter.Printf("    Ads:        %s\n", strings.Join(c.Ads, ", "))
	infoPrinter.Printf("    Analytics:  %s\n", c.Analytics)
}
