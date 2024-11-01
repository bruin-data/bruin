package s3

type Config struct {
	BucketName      string
	PathToFile      string
	AccessKeyID     string
	SecretAccessKey string
}

// s3://<bucket_name>/<path_to_file>?access_key_id=<access_key_id>&secret_access_key=<secret_access_key>
func (c *Config) GetIngestrURI() string {
	return "s3://" + c.BucketName + "/" + c.PathToFile + "?access_key_id=" + c.AccessKeyID + "&secret_access_key=" + c.SecretAccessKey
}
