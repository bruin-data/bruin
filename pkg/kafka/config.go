package kafka

type Config struct {
	BootstrapServers string
	GroupId          string
	SecurityProtocol string
	SaslMechanisms   string
	SaslUsername     string
	SaslPassword     string
	BatchSize        string
	BatchTimeout     string
}

func (c *Config) GetIngestrURI() string {
	uri := "kafka://"

	if c.BootstrapServers != "" {
		uri += "?bootstrap_servers=" + c.BootstrapServers
	}

	if c.GroupId != "" {
		uri += "&group_id=" + c.GroupId
	}

	if c.SecurityProtocol != "" {
		uri += "&security_protocol=" + c.SecurityProtocol
	}
	if c.SaslMechanisms != "" {
		uri += "&sasl_mechanisms=" + c.SaslMechanisms
	}
	if c.SaslUsername != "" {
		uri += "&sasl_username=" + c.SaslUsername
	}
	if c.SaslPassword != "" {
		uri += "&sasl_password=" + c.SaslPassword
	}
	if c.BatchSize != "" {
		uri += "&batch_size=" + c.BatchSize
	}
	if c.BatchTimeout != "" {
		uri += "&batch_timeout=" + c.BatchTimeout
	}

	return uri
}
