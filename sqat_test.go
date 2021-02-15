package sqat_test

import (
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/fujiwara/sqat"
	"github.com/google/go-cmp/cmp"
)

var testMessages = []struct {
	message   *sqs.Message
	timestamp int64
}{
	{
		message:   &sqs.Message{},
		timestamp: 0,
	},
	{
		message: &sqs.Message{
			MessageAttributes: map[string]*sqs.MessageAttributeValue{
				"AtTimestamp": {
					DataType:    aws.String("Number"),
					StringValue: aws.String("1613142260"),
				},
			},
		},
		timestamp: 1613142260,
	},
	{
		message: &sqs.Message{
			MessageAttributes: map[string]*sqs.MessageAttributeValue{
				"AtTimestamp": {
					DataType:    aws.String("String"),
					StringValue: aws.String("2021-02-13T01:21:55+09:00"),
				},
			},
		},
		timestamp: 1613146915,
	},
	{
		message: &sqs.Message{
			MessageAttributes: map[string]*sqs.MessageAttributeValue{
				"AtTimestamp": {
					DataType: aws.String("Binary"),
				},
			},
		},
		timestamp: 0,
	},
}

var testLambdaMessages = []struct {
	src      events.SQSMessage
	expected sqs.Message
}{
	{
		src: events.SQSMessage{
			Attributes: map[string]string{
				"ApproximateFirstReceiveTimestamp": "1613362867488",
				"ApproximateReceiveCount":          "1",
				"SenderId":                         "AROAJPKWIXB7ME2EZEO3G:botocore-session-1613362839",
				"SentTimestamp":                    "1613362867484",
			},
			Body:                   "foobar2",
			Md5OfBody:              "cbac77acf401ef89c3297bd9fc41ec0b",
			Md5OfMessageAttributes: "9fd5e217089075be61cd1d67e35e5117",
			MessageAttributes: map[string]events.SQSMessageAttribute{
				"AtTimestamp": {
					DataType:         "String",
					StringValue:      aws.String("2021-02-15T12:00:33+09:00"),
					StringListValues: []string{},
					BinaryListValues: [][]byte{},
				},
			},
			MessageId:     "206e2dbd-655b-4538-be95-d0596a3b1c3f",
			ReceiptHandle: "AQEB1/nS93vgYfxjM84fTryjYa18A5U+Os",
		},
		expected: sqs.Message{
			Attributes: map[string]*string{
				"ApproximateFirstReceiveTimestamp": aws.String("1613362867488"),
				"ApproximateReceiveCount":          aws.String("1"),
				"SenderId":                         aws.String("AROAJPKWIXB7ME2EZEO3G:botocore-session-1613362839"),
				"SentTimestamp":                    aws.String("1613362867484"),
			},
			Body:                   aws.String("foobar2"),
			MD5OfBody:              aws.String("cbac77acf401ef89c3297bd9fc41ec0b"),
			MD5OfMessageAttributes: aws.String("9fd5e217089075be61cd1d67e35e5117"),
			MessageAttributes: map[string]*sqs.MessageAttributeValue{
				"AtTimestamp": {
					DataType:    aws.String("String"),
					StringValue: aws.String("2021-02-15T12:00:33+09:00"),
				},
			},
			MessageId:     aws.String("206e2dbd-655b-4538-be95-d0596a3b1c3f"),
			ReceiptHandle: aws.String("AQEB1/nS93vgYfxjM84fTryjYa18A5U+Os"),
		},
	},
}

func TestExtractTimestamp(t *testing.T) {
	for _, tc := range testMessages {
		ts, err := sqat.ExtractTimestamp(tc.message)
		if tc.timestamp == 0 {
			t.Log(err)
			if err == nil {
				t.Error("error must be occured")
			}
		} else if tc.timestamp != ts {
			if err != nil {
				t.Error(err)
			}
			t.Errorf("unexpected timestamp: got %d expected %d", ts, tc.timestamp)
		}
	}
}

func TestLambdaMessages(t *testing.T) {
	for _, tc := range testLambdaMessages {
		msg, err := sqat.EventToMessage(tc.src)
		if err != nil {
			t.Error(err)
		}
		if diff := cmp.Diff(msg, &tc.expected); diff != "" {
			t.Error(diff)
		}
	}
}
