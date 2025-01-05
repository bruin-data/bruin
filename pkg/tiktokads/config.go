package tiktokads

type Config struct {
	AccessToken   string
	AdvertiserIDs string
	Timezone      string
}

func (c Config) GetIngestrURI() string {
	return "tiktok://?access_token=" + c.AccessToken + "&advertiser_ids=" + c.AdvertiserIDs + "&timezone=" + c.Timezone
}
