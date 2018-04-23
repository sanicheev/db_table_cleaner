package helpers

import (
	"net/mail"
	"net/smtp"
	"fmt"
	log "github.com/sirupsen/logrus"
)

type Payload struct {
	From mail.Address
	To mail.Address
	Subject string
	body string
}

func (p *Payload) SetSender(Address string) {
	log.Debug(fmt.Sprintf("Setting From to: %s", Address))
	p.From = mail.Address{"", Address}
}

func (p *Payload) SetDestination(Address string) {
	log.Debug(fmt.Sprintf("Setting To to: %s", Address))
	p.To = mail.Address{"", Address}
}

func (p *Payload) SetSubject(Subject string) {
	log.Debug(fmt.Sprintf("Setting Subject to: %s", Subject))
	p.Subject = Subject
}

func (p *Payload) SetMessage(message string) (string) {
	log.Debug(fmt.Sprintf("Setting Message to: %s", message))
	return message
}

func (p *Payload) SetHead() (string) {
	var head string
	headers := make(map[string]string)
	headers["From"] = p.From.String()
	headers["To"] = p.To.String()
	headers["Subject"] = p.Subject
	for k,v := range headers {
		head += fmt.Sprintf("%s: %s\r\n", k, v)
	}
	head += "\r\n"
	log.Debug(fmt.Sprintf("Setting Head to: %s", head))
	return head
}

func (p *Payload) SetBody(msg string) {
	head := p.SetHead()
	message := p.SetMessage(msg)
	p.body = head + message
	log.Debug(fmt.Sprintf("Setting Body to: %s", p.body))
}

func SendEmail(socket string, payload Payload){
	connection, err := smtp.Dial(socket)
	log.Debug(fmt.Sprintf("Opening connection to SMTP server: %s", socket))
	LogError(err)
	defer connection.Quit()

	err = connection.Mail(payload.From.Address)
	LogError(err)

	connection.Rcpt(payload.To.Address)
	LogError(err)

	writer, err := connection.Data()
	LogError(err)

	_, err = writer.Write([]byte(payload.body))
	LogError(err)

	defer writer.Close()
}
