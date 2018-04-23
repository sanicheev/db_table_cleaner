package helpers

import (
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
	"fmt"
)

func ParseYAML(filename string, config *Config) {
	err := yaml.Unmarshal(ReadFile(filename), config)
	log.Debug(fmt.Sprintf("Parsing YAML file: %s", filename))
	LogError(err)
}