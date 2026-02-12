package cute

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type Config struct {
	Engine string `json:"engine"`
	Millis int    `json:"millis"`
}

func FindConfigPath() (string, string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", "", err
	}
	paths := []string{
		filepath.Join(cwd, "config.json"),
		filepath.Join(cwd, "..", "config.json"),
	}
	for _, path := range paths {
		if _, err := os.Stat(path); err == nil {
			return path, filepath.Dir(path), nil
		}
	}
	return "", "", fmt.Errorf("config.json not found from %s", cwd)
}

func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}
