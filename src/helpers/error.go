package helpers

import (
	log "github.com/sirupsen/logrus"
)

func LogErrorVar(err error, status *bool) {
	if err != nil {
		log.Error(err)
		*status = true
	}
}

func LogError(err error) {
	if err != nil {
		log.Error(err)
	}
}