package main

import(
	"os"
	"flag"
	log "github.com/sirupsen/logrus"
	"helpers"
	t "tables/Common"
	"fmt"
)

const(
	to = "alerts@example.com"
	from = "root@localhost"
	subject = "Cleanup results"
	defaultSMTP = "localhost:25"
	separator = "\t#############################################\n"
	defaultConfigDir = "configs"
	commonHandler = "common"
)

var (
	LogLevel int
	MailTo string
	MailFrom string
	SMTPHost string
	ConfigDir string
	Handler map[string]interface{}
)

func init(){
	// Initialize logging
	log.SetOutput(os.Stdout)

	// Initialize flags parser
	flag.IntVar(&LogLevel, "log", 3, "Set log level")
	flag.StringVar(&MailTo, "mailto", to, "Set mail sender address")
	flag.StringVar(&MailFrom, "mailfrom", from, "Set mail recipient address")
	flag.StringVar(&SMTPHost, "smtphost", defaultSMTP, "Set smtp server address")
	flag.StringVar(&ConfigDir, "configdir", defaultConfigDir, "Set custom configuratio dir")

	// Initialize handlers
	Handler = make(map[string]interface{})
	Handler[commonHandler] = &t.Common{}
}

func GetLogLevel(level int) (log.Level) {
	switch(level) {
	case 0:
		return log.PanicLevel
	case 1:
		return log.FatalLevel
	case 2:
		return log.ErrorLevel
	case 3:
		return log.WarnLevel
	case 4:
		return log.InfoLevel
	case 5:
		return log.DebugLevel
	default:
		return log.WarnLevel
	}
}

func CleanTable(r helpers.Clean, config *helpers.Config, stats map[string]helpers.TableStats, ch chan<- bool) {
	connection, err := helpers.OpenConnection(
		config.Username,
		helpers.GetEnvVar(config.Password),
		config.Host,
		config.Database,
	)
	if err != nil {
		log.Error("Failed to open DB connection to: %s", config.Database)
		os.Exit(1)
	}
	defer connection.Close()
	log.Debug(fmt.Sprintf("Cleaning table: %s", config.TableName))
	r.Prepare(connection, config)
	r.Run(config)
	stats[config.TableName] = r.GetTableStats()
	ch <- true
}

func ProcessResults(stats map[string]helpers.TableStats) string {
	log.Debug(fmt.Sprintf("Parsing cleanup stats"))
	var body string
	body += "Results of cleanup operation:\n"
	body += separator
	for table,metrics := range stats {
		body += fmt.Sprintf("\tStats for table: %s\n", table)
		body += fmt.Sprintf("\t\tFailed: %v\n", metrics.ExitStatus)
		body += fmt.Sprintf("\t\tRows processed: %d\n", metrics.RowsProcessed)
		body += fmt.Sprintf("\t\tTime taken: %v\n", metrics.TimeTaken)
		body += fmt.Sprintf("\t\tFragmentation required: %v\n", metrics.FragmentationRequired)
		body += separator
	}
	log.Debug(fmt.Sprintf("Composed body: %s", body))
	return body
}

func getHandler(t string) interface{} {
	return Handler[t]
}

func Notify(results string) {
	payload := helpers.Payload{}
	payload.SetSender(MailFrom)
	payload.SetDestination(MailTo)
	payload.SetSubject(subject)
	payload.SetBody(results)
	log.Debug(fmt.Sprintf("Connecting to SMTP server: %s and sending notification", SMTPHost))
	helpers.SendEmail(SMTPHost, payload)
}

func main() {
	flag.Parse()
	log.SetLevel(GetLogLevel(LogLevel))

	stats := map[string]helpers.TableStats{}
	ch := make(chan bool)

	for _,configPath := range helpers.GetYAMLFiles(ConfigDir) {
		config := helpers.Config{}
		helpers.ParseYAML(
			configPath,
			&config,
		)

		t := getHandler(config.Type)
		go CleanTable(t.(helpers.Clean), &config, stats, ch)
	}
	<-ch

	Notify(ProcessResults(stats))
}
