package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/xml"
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func TestFIFOHighThroughputAndSHA256(t *testing.T) {
	t.Parallel()
	client := newSQSClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	queue := deliveryQueue(t, client, ctx, map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "true", "DeduplicationScope": "messageGroup", "FifoThroughputLimit": "perMessageGroupId"})
	attrs := pollingAttributes(t, client, ctx, queue)
	if attrs["DeduplicationScope"] != "messageGroup" || attrs["FifoThroughputLimit"] != "perMessageGroupId" {
		t.Fatalf("attributes: %v", attrs)
	}
	body := "日本語<&>"
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte(body)))
	sent, err := client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: queue, Entries: []types.SendMessageBatchRequestEntry{
		{Id: aws.String("a"), MessageBody: aws.String(body), MessageGroupId: aws.String("a")},
		{Id: aws.String("b"), MessageBody: aws.String(body), MessageGroupId: aws.String("b")},
		{Id: aws.String("duplicate"), MessageBody: aws.String("override body"), MessageGroupId: aws.String("a"), MessageDeduplicationId: aws.String(hash)},
	}})
	if err != nil || len(sent.Successful) != 3 || len(sent.Failed) != 0 {
		t.Fatalf("batch: %+v %v", sent, err)
	}
	if aws.ToString(sent.Successful[0].MessageId) == aws.ToString(sent.Successful[1].MessageId) || aws.ToString(sent.Successful[0].MessageId) != aws.ToString(sent.Successful[2].MessageId) {
		t.Fatalf("scope/dedup: %+v", sent.Successful)
	}
	received, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: queue, MaxNumberOfMessages: 10, MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll}})
	if err != nil || len(received.Messages) != 2 {
		t.Fatalf("receive: %+v %v", received, err)
	}
	for _, message := range received.Messages {
		if message.Attributes["MessageDeduplicationId"] != hash {
			t.Fatalf("SHA256: %+v", message)
		}
		deleteMessage(t, ctx, client, queue, message.ReceiptHandle)
	}
	for _, invalid := range []map[string]string{
		{"DeduplicationScope": "queue"}, {"DeduplicationScope": "invalid"}, {"FifoThroughputLimit": "invalid"},
	} {
		_, err := client.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{QueueUrl: queue, Attributes: invalid})
		requireHTTPError(t, err, "InvalidParameterValue")
		if !reflect.DeepEqual(pollingAttributes(t, client, ctx, queue), attrs) {
			t.Fatal("invalid settings mutated queue")
		}
	}
	// Change both dependent settings atomically using the Query protocol.
	postQuery(t, lqsEndpoint(), url.Values{"Action": {"SetQueueAttributes"}, "QueueUrl": {*queue}, "Attribute.1.Name": {"DeduplicationScope"}, "Attribute.1.Value": {"queue"}, "Attribute.2.Name": {"FifoThroughputLimit"}, "Attribute.2.Value": {"perQueue"}})
	attrs = pollingAttributes(t, client, ctx, queue)
	if attrs["DeduplicationScope"] != "queue" || attrs["FifoThroughputLimit"] != "perQueue" {
		t.Fatal(attrs)
	}
	duplicate, err := client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: queue, MessageBody: aws.String(body), MessageGroupId: aws.String("c")})
	if err != nil || aws.ToString(duplicate.MessageId) != aws.ToString(sent.Successful[0].MessageId) {
		t.Fatalf("deleted dedup key: %+v %v", duplicate, err)
	}
	standard := deliveryQueue(t, client, ctx, nil)
	_, err = client.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{QueueUrl: standard, Attributes: map[string]string{"DeduplicationScope": "queue"}})
	requireHTTPError(t, err, "InvalidParameterValue")
}

func TestFIFOReceiveAttemptSDKAndQuery(t *testing.T) {
	t.Parallel()
	client := newSQSClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	queue := deliveryQueue(t, client, ctx, map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "true", "LqsMaxInFlightMessages": "1"})
	_, err := client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: queue, MessageBody: aws.String("first"), MessageGroupId: aws.String("g"), MessageAttributes: sampleAttributes()})
	if err != nil {
		t.Fatal(err)
	}
	input := &sqs.ReceiveMessageInput{QueueUrl: queue, ReceiveRequestAttemptId: aws.String("attempt-example"), VisibilityTimeout: 2, MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll}, MessageAttributeNames: []string{"All"}}
	first, err := client.ReceiveMessage(ctx, input)
	if err != nil || len(first.Messages) != 1 {
		t.Fatalf("first: %+v %v", first, err)
	}
	time.Sleep(1200 * time.Millisecond)
	retry, err := client.ReceiveMessage(ctx, input)
	if err != nil || !reflect.DeepEqual(first.Messages, retry.Messages) {
		t.Fatalf("retry: %+v %v", retry, err)
	}
	time.Sleep(1100 * time.Millisecond)
	empty, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: queue})
	if err != nil || len(empty.Messages) != 0 {
		t.Fatalf("visibility not reset: %+v %v", empty, err)
	}
	wire := postQuery(t, lqsEndpoint(), url.Values{"Action": {"ReceiveMessage"}, "QueueUrl": {*queue}, "ReceiveRequestAttemptId": {"attempt-example"}, "VisibilityTimeout": {"2"}, "MessageSystemAttributeName.1": {"All"}})
	var result struct {
		Messages []struct {
			ID      string `xml:"MessageId"`
			Receipt string `xml:"ReceiptHandle"`
		} `xml:"ReceiveMessageResult>Message"`
	}
	if err := xml.Unmarshal(wire, &result); err != nil || len(result.Messages) != 1 || result.Messages[0].Receipt != aws.ToString(first.Messages[0].ReceiptHandle) {
		t.Fatalf("Query replay: %s %v", wire, err)
	}
	_, err = client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{QueueUrl: queue, ReceiptHandle: first.Messages[0].ReceiptHandle, VisibilityTimeout: 0})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.ReceiveMessage(ctx, input)
	requireHTTPError(t, err, "InvalidParameterValue")
	input.ReceiveRequestAttemptId = aws.String("next-example")
	next, err := client.ReceiveMessage(ctx, input)
	if err != nil || len(next.Messages) != 1 || next.Messages[0].Attributes["ApproximateReceiveCount"] != "2" {
		t.Fatalf("next: %+v %v", next, err)
	}
	deleteMessage(t, ctx, client, queue, next.Messages[0].ReceiptHandle)
	_, err = client.ReceiveMessage(ctx, input)
	requireHTTPError(t, err, "InvalidParameterValue")
	for _, id := range []string{"", "bad space", strings.Repeat("x", 129)} {
		input.ReceiveRequestAttemptId = aws.String(id)
		_, err = client.ReceiveMessage(ctx, input)
		requireHTTPError(t, err, "InvalidParameterValue")
	}
	standard := deliveryQueue(t, client, ctx, nil)
	_, err = client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: standard, ReceiveRequestAttemptId: aws.String("attempt-example")})
	requireHTTPError(t, err, "InvalidParameterValue")
}

func TestFIFOEmptyAttemptWaitsOnlyOnce(t *testing.T) {
	t.Parallel()
	client := newSQSClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	queue := deliveryQueue(t, client, ctx, map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "true"})
	input := &sqs.ReceiveMessageInput{QueueUrl: queue, WaitTimeSeconds: 1, ReceiveRequestAttemptId: aws.String("empty-example")}
	start := time.Now()
	first, err := client.ReceiveMessage(ctx, input)
	if err != nil || len(first.Messages) != 0 || time.Since(start) < time.Second {
		t.Fatalf("initial wait: %+v %v", first, err)
	}
	_, err = client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: queue, MessageBody: aws.String("later"), MessageGroupId: aws.String("g")})
	if err != nil {
		t.Fatal(err)
	}
	start = time.Now()
	retry, err := client.ReceiveMessage(ctx, input)
	if err != nil || len(retry.Messages) != 0 || time.Since(start) > 700*time.Millisecond {
		t.Fatalf("empty replay: %+v %v", retry, err)
	}
	input.ReceiveRequestAttemptId = aws.String("new-example")
	received, err := client.ReceiveMessage(ctx, input)
	if err != nil || len(received.Messages) != 1 {
		t.Fatalf("new receive: %+v %v", received, err)
	}
}
