package sqat

import (
	"context"
	"encoding/json"
	"log"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"
)

// LambdaHandler generates a handler for Lambda.
func (r *Router) LambdaHandler() func(ctx context.Context, event events.SQSEvent) error {
	return func(ctx context.Context, event events.SQSEvent) error {
		for _, m := range event.Records {
			msg, err := eventToMessage(m)
			if err != nil {
				log.Println("[error]", err)
				return err
			}
			if err := r.HandleMessage(ctx, msg); err != nil {
				log.Println("[error]", err)
				return err
			}
		}
		return nil
	}
}

func eventToMessage(m events.SQSMessage) (*sqs.Message, error) {
	b, err := json.Marshal(m)
	if err != nil {
		log.Println("[error]", err)
		return nil, errors.Wrap(err, "failed to marshal JSON")
	}
	var msg sqs.Message
	if err := json.Unmarshal(b, &msg); err != nil {
		log.Println("[error]", err)
		return nil, errors.Wrap(err, "failed to unmarshal JSON")
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
	return &msg, nil
}
