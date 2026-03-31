package quicksight

type Config struct {
	Name               string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	AwsAccessKeyID     string `yaml:"aws_access_key_id,omitempty" json:"aws_access_key_id" mapstructure:"aws_access_key_id"`
	AwsSecretAccessKey string `yaml:"aws_secret_access_key,omitempty" json:"aws_secret_access_key" mapstructure:"aws_secret_access_key"`
	AwsSessionToken    string `yaml:"aws_session_token,omitempty" json:"aws_session_token" mapstructure:"aws_session_token"`
	AwsRegion          string `yaml:"aws_region,omitempty" json:"aws_region" mapstructure:"aws_region"`
	AwsAccountID       string `yaml:"aws_account_id,omitempty" json:"aws_account_id" mapstructure:"aws_account_id"`
}

func (c Config) GetName() string {
	return c.Name
}
