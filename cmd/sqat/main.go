package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/fujiwara/sqat"
	"github.com/hashicorp/logutils"
	"github.com/pkg/errors"
)

func main() {
	r, err := setup()
	if err != nil {
		log.Println("[error]", err)
		os.Exit(1)
	}

	if strings.HasPrefix(os.Getenv("AWS_EXECUTION_ENV"), "AWS_Lambda") ||
		os.Getenv("AWS_LAMBDA_RUNTIME_API") != "" {
		lambda.Start(lambdaHandler(r))
		return
	}
	if err := cli(r); err != nil {
		log.Println("[error]", err)
		os.Exit(1)
	}
}

func lambdaHandler(r *sqat.Router) func(context.Context, events.SQSEvent) error {
	return func(ctx context.Context, event events.SQSEvent) error {
		for _, m := range event.Records {
			b, err := json.Marshal(m)
			if err != nil {
				log.Println("[error]", err)
				return errors.Wrap(err, "failed to marshal JSON")
			}
			var msg sqs.Message
			if err := json.Unmarshal(b, &msg); err != nil {
				log.Println("[error]", err)
				return errors.Wrap(err, "failed to unmarshal JSON")
			}
			for key, attr := range msg.MessageAttributes {
				if attr.BinaryListValues != nil {
					if len(attr.BinaryListValues) > 0 {
						log.Printf("[warn] purge BinaryListValues in message attribute %s. Message attribute list values in SendMessage operation are not supported.", key)
					}
					attr.BinaryListValues = nil
				}
				if attr.StringListValues != nil {
					if len(attr.StringListValues) > 0 {
						log.Printf("[warn] purge StringListValues in message attribute %s. Message attribute list values in SendMessage operation are not supported.", key)
					}
					attr.StringListValues = nil
				}
			}
			if err := r.HandleMessage(ctx, &msg); err != nil {
				log.Println("[error]", err)
				return err
			}
		}
		return nil
	}
}

func setup() (*sqat.Router, error) {
	var (
		inQueue, outQueue, logLevel string
	)
	flag.StringVar(&inQueue, "in", "", "Incoming SQS queue URL")
	flag.StringVar(&outQueue, "out", "", "Outgoing SQS queue URL")
	flag.StringVar(&logLevel, "log-level", "info", "log level")
	flag.VisitAll(envToFlag)
	flag.Parse()

	filter := &logutils.LevelFilter{
		Levels:   []logutils.LogLevel{"debug", "info", "warn", "error"},
		MinLevel: logutils.LogLevel(logLevel),
		Writer:   os.Stderr,
	}
	log.SetOutput(filter)

	if inQueue == "" {
		return nil, errors.New("incoming queue URL is required")
	}
	if outQueue == "" {
		return nil, errors.New("outgoing queue URL is required")
	}

	opt := sqat.Option{
		IncomingQueueURL: inQueue,
		OutgoingQueueURL: outQueue,
	}
	return sqat.New(&opt)
}

func cli(r *sqat.Router) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var trapSignals = []os.Signal{
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
	}
	var sigCh = make(chan os.Signal, 1)
	signal.Notify(sigCh, trapSignals...)
	go func() {
		sig := <-sigCh
		log.Println("[info] got signal", sig)
		cancel()
	}()

	return r.Poll(ctx)
}

func envToFlag(f *flag.Flag) {
	name := strings.ToUpper(strings.Replace(f.Name, "-", "_", -1))
	if s, ok := os.LookupEnv("SQAT_" + name); ok {
		f.Value.Set(s)
	}
}
