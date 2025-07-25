package ydbjobs

type Config struct {
	Endpoint          string             `mapstructure:"endpoint"`
	StaticCredentials *StaticCredentials `mapstructure:"static_credentials"`
	TLS               *TLS               `mapstructure:"tls"`

	Priority     int           `mapstructure:"priority"`
	Topic        string        `mapstructure:"topic"`
	ProducerOpts *ProducerOpts `mapstructure:"producer_options"`
	ConsumerOpts *ConsumerOpts `mapstructure:"consumer_options"`
}

type StaticCredentials struct {
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
}

type TLS struct {
	Ca string `mapstructure:"ca"`
}

type ProducerOpts struct {
	Id string `mapstructure:"id"`
}

type ConsumerOpts struct {
	Name string `mapstructure:"name"`
}
