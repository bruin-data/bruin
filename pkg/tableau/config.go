package tableau

type Config struct {
	Name                      string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	Host                      string `yaml:"host,omitempty" json:"host" mapstructure:"host"`
	Username                  string `yaml:"username,omitempty" json:"username" mapstructure:"username"`
	Password                  string `yaml:"password,omitempty" json:"password" mapstructure:"password"`
	PersonalAccessTokenName   string `yaml:"personal_access_token_name,omitempty" json:"personal_access_token_name" mapstructure:"personal_access_token_name"`
	PersonalAccessTokenSecret string `yaml:"personal_access_token_secret,omitempty" json:"personal_access_token_secret" mapstructure:"personal_access_token_secret"`
	SiteID                    string `yaml:"site_id,omitempty" json:"site_id" mapstructure:"site_id"`
	APIVersion                string `yaml:"api_version,omitempty" json:"api_version" mapstructure:"api_version"`
}

func (c Config) GetName() string {
	return c.Name
}
