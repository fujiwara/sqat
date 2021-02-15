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
	// AtTimestamp is a message attribute key represents an invocation timestamp.
	AtTimestamp = "AtTimestamp"

	// OriginalMessageID is a message attribute key represents an original message ID.
	OriginalMessageID = "OriginalMessageID"

	sqsMaxDelaySeconds = 900
)

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
}

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
	log.Println("[info] polling incoming queue", r.option.IncomingQueueURL)
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
			if err := r.HandleMessage(ctx, m); err != nil {
				log.Println("[error] failed to handle message.", err)
			}
		}
	}
}

// HandleMessage routes a message.
func (r *Router) HandleMessage(ctx context.Context, msg *sqs.Message) error {
	log.Println("[debug] received message", msg.GoString())
	invokedAt, err := extractTimestamp(msg)
	if err != nil {
		log.Println("[warn]", err)
	}

	var originalMessageID string
	if mid := msg.MessageAttributes[OriginalMessageID]; mid == nil {
		log.Printf("[debug] set original message id to message attribute OriginalMessageId")
		msg.MessageAttributes[OriginalMessageID] = &sqs.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: msg.MessageId,
		}
		originalMessageID = aws.StringValue(msg.MessageId)
	} else {
		log.Printf("[debug] keep original message id")
		originalMessageID = aws.StringValue(mid.StringValue)
	}
	log.Printf(
		"[info] handle message %s, original %s",
		*msg.MessageId,
		originalMessageID,
	)

	delay := invokedAt - time.Now().Unix()
	if delay > 0 {
		if delay > sqsMaxDelaySeconds {
			log.Printf("[info] delay %d sec, truncate to %d sec", delay, sqsMaxDelaySeconds)
			delay = sqsMaxDelaySeconds
		} else {
			log.Printf("[info] delay %d sec", delay)
		}
		log.Printf("[info] requeue: %s", originalMessageID)
		// requeue to incoming queue
		if _, err := r.sqs.SendMessageWithContext(ctx, &sqs.SendMessageInput{
			QueueUrl:          aws.String(r.option.IncomingQueueURL),
			MessageBody:       msg.Body,
			MessageAttributes: msg.MessageAttributes,
			DelaySeconds:      aws.Int64(delay),
		}); err != nil {
			return errors.Wrap(err, "failed to requeue message to incoming")
		}
	} else {
		log.Printf("[info] move to outgoing: %s", originalMessageID)
		// put to outgoing queue
		if _, err := r.sqs.SendMessageWithContext(ctx, &sqs.SendMessageInput{
			QueueUrl:          aws.String(r.option.OutgoingQueueURL),
			MessageBody:       msg.Body,
			MessageAttributes: msg.MessageAttributes,
		}); err != nil {
			return errors.Wrap(err, "failed to send message to outgoing")
		}
	}

	log.Printf("[info] delete from incoming: %s", originalMessageID)
	if _, err := r.sqs.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(r.option.IncomingQueueURL),
		ReceiptHandle: msg.ReceiptHandle,
	}); err != nil {
		return errors.Wrap(err, "failed to delete message from incoming")
	}
	return nil
}

func extractTimestamp(msg *sqs.Message) (timestamp int64, err error) {
	defer func() {
		log.Println("[debug] extract timestamp", timestamp)
	}()
	at := msg.MessageAttributes[AtTimestamp]
	if at == nil {
		return 0, errors.New("no message attribute AtTimestamp")
	}
	switch dt := aws.StringValue(at.DataType); dt {
	case "Number":
		s := aws.StringValue(at.StringValue)
		timestamp, err = strconv.ParseInt(s, 10, 64)
		if err != nil {
			return 0, errors.Wrapf(err, "failed to Parse message attribute AtTimestamp as Integer %s", s)
		}
	case "String":
		s := aws.StringValue(at.StringValue)
		ts, err := time.Parse(time.RFC3339, s)
		if err != nil {
			return 0, errors.Wrapf(err, "failed to Parse message attribute AtTimestamp as RFC3339 %s", s)
		}
		timestamp = ts.Unix()
	default:
		return 0, errors.Errorf("invalid DataType of message attribute AtTimestamp: %s", dt)
	}
	return
}
