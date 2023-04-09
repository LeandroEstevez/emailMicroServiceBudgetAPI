package main

import (
	"bytes"
	"crypto/tls"
	"database/sql"
	"emailService/util"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/k3a/html2text"
	"gopkg.in/gomail.v2"
)

func main() {
	conf := util.ReadConfig("config.properties")

	consumer, err := kafka.NewConsumer(&conf)

	if err != nil {
		fmt.Printf("Failed to create consumer: %s", err)
		os.Exit(1)
	}

	topic := "email_topic"
	consumer.SubscribeTopics([]string{topic}, nil)
	// Set up a channel for handling Ctrl-C, etc
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Event containers
	var emailData emailMessage

	// Process messages
	run := true
	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			event, err := consumer.ReadMessage(100 * time.Millisecond)
			if err != nil {
				// Errors are informational and automatically handled by the consumer
				continue
			}
			fmt.Printf("Consumed event from topic %s: key = %-10s",
				*event.TopicPartition.Topic, string(event.Key))

			switch string(event.Key) {
			case "emailMessage":
				fmt.Println("Inside switch statement")
				err := json.Unmarshal(event.Value, &emailData)
				if err != nil {
					fmt.Println("Error parsing the data to json")
					// Errors are informational and automatically handled by the consumer
					continue
				}
			}

			err = nil
			errCounter := 1
			for errCounter <= 3 {
				err = sendEmail(&emailData.User, &emailData.EmailData, "resetPassword.html")
				if err != nil {
					fmt.Printf("Error sending the email, will try %#v more times", 3-errCounter)
					errCounter++
					continue
				}
				break
			}
		}
	}

	consumer.Close()

}

type Entry struct {
	ID      int32     `json:"id"`
	Owner   string    `json:"owner"`
	Name    string    `json:"name"`
	DueDate time.Time `json:"due_date"`
	// must be positive
	Amount   int64          `json:"amount"`
	Category sql.NullString `json:"category"`
}

type emailMessage struct {
	User      User      `json:"user"`
	EmailData emailData `json:"emailData"`
}

type User struct {
	Username          string    `json:"username"`
	HashedPassword    string    `json:"hashed_password"`
	FullName          string    `json:"full_name"`
	Email             string    `json:"email"`
	TotalExpenses     int64     `json:"total_expenses"`
	PasswordChangedAt time.Time `json:"password_changed_at"`
	CreatedAt         time.Time `json:"created_at"`
}

type emailData struct {
	URL       string `json:"url"`
	FirstName string `json:"first_name"`
	Subject   string `json:"subject"`
}

// ? Email template parser
func parseTemplateDir(dir string) (*template.Template, error) {
	var paths []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			paths = append(paths, path)
		}
		return nil
	})

	fmt.Println("Am parsing templates...")

	if err != nil {
		return nil, err
	}

	return template.ParseFiles(paths...)
}

// ? Email template parser
func sendEmail(user *User, data *emailData, templateName string) error {
	fmt.Println("Sending email!!!!!!")

	// Sender data.
	from := "leandroest111298@gmail.com"
	smtpPass := "npluhcjauhqfwsag"
	smtpUser := "leandroest111298@gmail.com"
	to := "leoest.dev@gmail.com"
	smtpHost := "smtp.gmail.com"
	smtpPort := 587

	var body bytes.Buffer

	template, err := parseTemplateDir("templates")
	if err != nil {
		log.Fatal("Could not parse template", err)
	}

	fmt.Println("Parsed the template")

	template = template.Lookup(templateName)
	template.Execute(&body, &data)
	fmt.Println(template.Name())

	fmt.Println("Got the template")

	m := gomail.NewMessage()

	m.SetHeader("From", from)
	m.SetHeader("To", to)
	m.SetHeader("Subject", data.Subject)
	m.SetBody("text/html", body.String())
	m.AddAlternative("text/plain", html2text.HTML2Text(body.String()))

	d := gomail.NewDialer(smtpHost, smtpPort, smtpUser, smtpPass)
	d.TLSConfig = &tls.Config{InsecureSkipVerify: true}

	// Send Email
	if err := d.DialAndSend(m); err != nil {
		return err
	}
	fmt.Println("Sent the email")
	return nil
}
