package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/aws/aws-lambda-go/lambda"
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
		lambda.Start(r.LambdaHandler())
		return
	}
	if err := run(r); err != nil {
		log.Println("[error]", err)
		os.Exit(1)
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

func run(r *sqat.Router) error {
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
