package datahose

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"os/user"
	"path"
)

type config struct {
	ServiceHost string `json:"service_host"`
	Email       string `json:"email"`
	Password    string `json:"password"`
}

func getConfigPath() (string, error) {
	usr, err := user.Current()
	if err != nil {
		return "", err
	}

	return path.Join(usr.HomeDir, ".config", "dalloriam", "datahose.json"), nil
}

func getConfig() (config, error) {
	cfgPath, err := getConfigPath()
	if err != nil {
		return config{}, err
	}

	if _, err := os.Stat(cfgPath); err != nil {
		return config{}, err
	}

	cfgData, err := ioutil.ReadFile(cfgPath)
	if err != nil {
		return config{}, err
	}

	var cfg config
	if err := json.Unmarshal(cfgData, &cfg); err != nil {
		return config{}, err
	}

	return cfg, nil
}
