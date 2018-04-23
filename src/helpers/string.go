package helpers

import (
	"strings"
	"fmt"
	"strconv"
	log "github.com/sirupsen/logrus"
)

func FormatString(format string, args ...string) string {
	boilerplate := strings.NewReplacer(args ...)
	out := boilerplate.Replace(format)
	log.Debug(fmt.Sprintf("Formatted string: %s", out))
	return out
}

func ArrayToString(array []int, delimiter string) string {
	log.Debug(fmt.Sprintf("Converting array: %v to string with delimiter: %s", array, delimiter))
	return strings.Trim(strings.Join(strings.Fields(fmt.Sprint(array)), delimiter), "[]")
}

func IntToString(value int) string {
	log.Debug(fmt.Sprintf("Converting integer: %d to string", value))
	return strconv.Itoa(value)
}