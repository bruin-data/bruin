package emr_serverless

type Config struct {
	AccessKey     string `yaml:"access_key"`
	SecretKey     string `yaml:"secret_key"`
	ApplicationID string `yaml:"application_id"`
	ExecutionRole string `yaml:"execution_role"`
	Region        string `yaml:"region"`
}

type Client struct {
	Config
}
