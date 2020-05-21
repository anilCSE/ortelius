// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package cfg

import (
	"time"

	"github.com/spf13/viper"
)

const (
	configKeysIPCRoot = "ipcRoot"

	configKeysFilter    = "filter"
	configKeysFilterMin = "min"
	configKeysFilterMax = "max"

	configKeysKafka          = "kafka"
	configKeysKafkaBrokers   = "brokers"
	configKeysKafkaGroupName = "GroupName"
)

// ClientConfig manages configuration data for the client app
type ClientConfig struct {
	Common

	KafkaConfig
	FilterConfig
	IPCRoot   string
	StartTime time.Time
}

type KafkaConfig struct {
	Brokers   []string
	GroupName string
}

type FilterConfig struct {
	Min uint32
	Max uint32
}

// NewClientConfig returns a *ClientConfig populated with data from the given file
func NewClientConfig(file string) (ClientConfig, error) {
	// Parse config file with viper and set defaults
	v, err := getConfigViper(file, map[string]interface{}{
		configKeysKafkaBrokers: "127.0.0.1:9092",
		configKeysFilter: map[string]interface{}{
			"max": 1073741824,
			"min": 2147483648,
		},
	})
	if err != nil {
		return ClientConfig{}, err
	}

	common, err := getCommonConfig(v)
	if err != nil {
		return ClientConfig{}, err
	}

	// Collect config data into a ClientConfig object
	return ClientConfig{
		Common: common,

		IPCRoot:      v.GetString(configKeysIPCRoot),
		KafkaConfig:  getKafkaConf(getSubViper(v, configKeysKafka)),
		FilterConfig: getFilterConf(getSubViper(v, configKeysFilter)),
	}, nil
}

func getFilterConf(v *viper.Viper) FilterConfig {
	if v == nil {
		return FilterConfig{}
	}
	return FilterConfig{
		Min: v.GetUint32(configKeysFilterMin),
		Max: v.GetUint32(configKeysFilterMax),
	}
}

func getKafkaConf(v *viper.Viper) KafkaConfig {
	if v == nil {
		return KafkaConfig{}
	}
	return KafkaConfig{
		Brokers:   v.GetStringSlice(configKeysKafkaBrokers),
		GroupName: v.GetString(configKeysKafkaGroupName),
	}
}
