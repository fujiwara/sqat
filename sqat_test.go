package sqat_test

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/fujiwara/sqat"
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
