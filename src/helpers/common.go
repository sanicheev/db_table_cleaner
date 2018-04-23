package helpers

import (
	"os"
	log "github.com/sirupsen/logrus"
	"fmt"
	"time"
	"database/sql"
	"io/ioutil"
	"path/filepath"
	"regexp"
)

const(
	regexpYAML = "(.*).yml"
	configPath = "{CONFIG_DIR}/{CONFIG_FILE}"
)

type Clean interface {
	Run(config *Config)
	Prepare(connection *sql.DB, config *Config)
	GetTableDesc() Table
	GetTableStats() TableStats
}

type TableStats struct {
	RowsProcessed int
	TimeTaken time.Duration
	ExitStatus bool
	FragmentationRequired bool
	StartTime time.Time
	EndTime time.Time
}

type Table struct {
	TableName string
	DeleteQuery string
	SelectQuery string
	Connection *sql.DB
	SelectThreshold int
	ResultLimit int
}

type Config struct {
	TableName string `yaml:"table"`
	Database string `yaml:"database"`
	Host string `yaml:"host"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	ResultLimit int `yaml:"limit"`
	SelectThreshold int `yaml:"threshold"`
	SelectQuery string `yaml:"select_query"`
	DeleteQuery string `yaml:"delete_query"`
	Type string `yaml:"type"`
}

func GetEnvVar(name string) (string) {
	 value, _ := os.LookupEnv(name)
	 log.Debug(fmt.Sprintf("Environment variable: %s has value: %s", name, value))
	 return value
}

func CountPercent(value1 float64, value2 float64) float64 {
	log.Debug(fmt.Sprintf("Received request to count percent of %f in %f", value1, value2))
	return value1/value2 * 100
}

func ReadFile(path string) []byte {
	log.Debug(fmt.Sprintf("Opening and reading file at path: %s", path))
	absolutePath, err := filepath.Abs(path)
	LogError(err)
	content, err := ioutil.ReadFile(absolutePath)
	LogError(err)
	return content
}

func GetConfigPath(format string, path string, filename string) string {
	return FormatString(
		format,
		"{CONFIG_DIR}", path,
		"{CONFIG_FILE}", filename,
	)
}

func GetYAMLFiles(configDir string) []string {
	dirContent, err := ioutil.ReadDir(configDir);
	var configList []string
	if err != nil {
		log.Panic(fmt.Sprintf("No directory: %s or its not accessible", configDir))
		os.Exit(1)
	}
	for _,file := range dirContent {
		fileName := file.Name()
		if matched,_ := regexp.MatchString(regexpYAML, fileName); matched == true {
			configList = append(
				configList,
				GetConfigPath(configPath, configDir, fileName),
			)
		}
	}
	return configList
}
