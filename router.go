package sqat

import (
	"context"
	"log"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"
)

const (
	AtTimestamp       = "AtTimestamp"
	OriginalMessageId = "OriginalMessageId"
)

// Router represents sqat application
type Router struct {
	sqs    *sqs.SQS
	option *Option
}

// New creates a new router
func New(opt *Option) (*Router, error) {
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}

	return &Router{
		sqs:    sqs.New(sess),
		option: opt,
	}, nil
}

// Poll polls incoming queue and do route.
func (r *Router) Poll(ctx context.Context) error {
	log.Println("[debug] polling incoming queue")
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		res, err := r.sqs.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
			MaxNumberOfMessages:   aws.Int64(1),
			QueueUrl:              aws.String(r.option.IncomingQueueURL),
			MessageAttributeNames: []*string{aws.String("All")},
		})
		if err != nil {
			log.Println("[warn]", err)
		}
		for _, m := range res.Messages {
			m := m
			log.Printf("[debug] message %s", m.GoString())
			attrs := make(map[string]string, len(m.MessageAttributes))
			for key, value := range m.MessageAttributes {
				if aws.StringValue(value.DataType) == "String" && value.StringValue != nil {
					attrs[key] = *value.StringValue
				}
			}
			msg := Message{
				ReceiptHandle: *m.ReceiptHandle,
				Body:          *m.Body,
				Attributes:    attrs,
				MessageId:     *m.MessageId,
			}
			if err := r.HandleMessage(ctx, msg); err != nil {
				log.Println("[warn]", err)
			}
		}
	}
}

func (r *Router) HandleMessage(ctx context.Context, msg Message) error {
	var invokedAt int64
	if at := msg.Attributes[AtTimestamp]; at != "" {
		ts, err := strconv.ParseInt(at, 10, 64)
		if err != nil {
			return errors.Wrapf(err, "failed to ParseInt message attribute 'invoked_at'")
		}
		invokedAt = ts
	}
	delay := invokedAt - time.Now().Unix()
	attrs := make(map[string]*sqs.MessageAttributeValue, len(msg.Attributes))
	for key, value := range msg.Attributes {
		attrs[key] = &sqs.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(value),
		}
	}
	if attrs[OriginalMessageId] == nil {
		attrs[OriginalMessageId] = &sqs.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(msg.MessageId),
		}
	}

	log.Printf("[debug] delay %d sec", delay)
	if delay <= 0 {
		log.Println("[debug] outgoing", msg.MessageId)
		// put to outgoing queue
		if _, err := r.sqs.SendMessageWithContext(ctx, &sqs.SendMessageInput{
			QueueUrl:          &r.option.OutgoingQueueURL,
			MessageBody:       &msg.Body,
			MessageAttributes: attrs,
		}); err != nil {
			return err
		}
	} else {
		log.Printf("[debug] reverting %s", msg.MessageId)
		// revert to incoming queue
		if _, err := r.sqs.SendMessageWithContext(ctx, &sqs.SendMessageInput{
			QueueUrl:          &r.option.IncomingQueueURL,
			MessageBody:       &msg.Body,
			DelaySeconds:      aws.Int64(delay),
			MessageAttributes: attrs,
		}); err != nil {
			return err
		}
	}
	log.Printf("[debug] deleting %s", msg.MessageId)
	if _, err := r.sqs.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      &r.option.IncomingQueueURL,
		ReceiptHandle: &msg.ReceiptHandle,
	}); err != nil {
		return err
	}
	return nil
}
