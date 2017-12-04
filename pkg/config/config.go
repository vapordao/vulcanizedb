package config

import (
	"os"

	"fmt"

	"path/filepath"

	"path"
	"runtime"

	"errors"

	"github.com/BurntSushi/toml"
)

type Config struct {
	Database Database
	Client   Client
}

var NewErrConfigFileNotFound = func(environment string) error {
	return errors.New(fmt.Sprintf("No configuration found for environment: %v", environment))
}

func NewConfig(environment string) (Config, error) {
	filenameWithExtension := fmt.Sprintf("%s.toml", environment)
	absolutePath := filepath.Join(ProjectRoot(), "environments", filenameWithExtension)
	config, err := parseConfigFile(absolutePath)
	if err != nil {
		return Config{}, NewErrConfigFileNotFound(environment)
	} else {
		if !filepath.IsAbs(config.Client.IPCPath) {
			config.Client.IPCPath = filepath.Join(ProjectRoot(), config.Client.IPCPath)
		}
		return config, nil
	}
}

func ProjectRoot() string {
	var _, filename, _, _ = runtime.Caller(0)
	return path.Join(path.Dir(filename), "..", "..")
}

func parseConfigFile(filePath string) (Config, error) {
	var cfg Config
	_, err := os.Stat(filePath)
	if err != nil {
		return Config{}, err
	} else {
		_, err := toml.DecodeFile(filePath, &cfg)
		if err != nil {
			return Config{}, err
		}
		return cfg, err
	}
}