package integration_test

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func pollingAttributes(t *testing.T, client *sqs.Client, ctx context.Context, queue *string) map[string]string {
	t.Helper()
	out, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: queue, AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameAll}})
	if err != nil {
		t.Fatal(err)
	}
	return out.Attributes
}

func TestPollingAttributesAndWaitOverrides(t *testing.T) {
	t.Parallel()
	client := newSQSClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	queue := deliveryQueue(t, client, ctx, nil)
	attrs := pollingAttributes(t, client, ctx, queue)
	if attrs["ReceiveMessageWaitTimeSeconds"] != "0" || attrs["LqsMaxInFlightMessages"] != "120000" || attrs["ApproximateNumberOfMessagesNotVisible"] != "0" {
		t.Fatalf("defaults: %v", attrs)
	}
	for _, attrs := range []map[string]string{
		{"ReceiveMessageWaitTimeSeconds": "21"}, {"ReceiveMessageWaitTimeSeconds": "-1"}, {"ReceiveMessageWaitTimeSeconds": "1.5"},
		{"LqsMaxInFlightMessages": "0"}, {"LqsMaxInFlightMessages": "120001"}, {"ReceiveMessageWaitTimeSeconds": "1", "LqsMaxInFlightMessages": "0"},
	} {
		_, err := client.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{QueueUrl: queue, Attributes: attrs})
		requireHTTPError(t, err, "InvalidParameterValue")
		_, err = client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(fmt.Sprintf("invalid-poll-%d", time.Now().UnixNano())), Attributes: attrs})
		requireHTTPError(t, err, "InvalidParameterValue")
		if pollingAttributes(t, client, ctx, queue)["ReceiveMessageWaitTimeSeconds"] != "0" {
			t.Fatal("invalid update changed wait time")
		}
	}
	_, err := client.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{QueueUrl: queue, Attributes: map[string]string{"ReceiveMessageWaitTimeSeconds": "1", "LqsMaxInFlightMessages": "2"}})
	if err != nil {
		t.Fatal(err)
	}
	attrs = pollingAttributes(t, client, ctx, queue)
	if attrs["ReceiveMessageWaitTimeSeconds"] != "1" || attrs["LqsMaxInFlightMessages"] != "2" {
		t.Fatalf("updated attrs: %v", attrs)
	}
	start := time.Now()
	out, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: queue})
	if err != nil || len(out.Messages) != 0 || time.Since(start) < time.Second || time.Since(start) > 3*time.Second {
		t.Fatalf("default wait: %+v %v (%v)", out, err, time.Since(start))
	}
	// Query explicitly serializes zero, overriding the queue's nonzero default.
	start = time.Now()
	postQuery(t, lqsEndpoint(), url.Values{"Action": {"ReceiveMessage"}, "QueueUrl": {*queue}, "WaitTimeSeconds": {"0"}})
	if time.Since(start) > 700*time.Millisecond {
		t.Fatal("explicit zero did not override queue wait")
	}
	for _, input := range []*sqs.ReceiveMessageInput{
		{QueueUrl: queue, WaitTimeSeconds: 21}, {QueueUrl: queue, WaitTimeSeconds: -1}, {QueueUrl: queue, MaxNumberOfMessages: 11},
	} {
		_, err := client.ReceiveMessage(ctx, input)
		requireHTTPError(t, err, "InvalidParameterValue")
	}
}

type pollingResult struct {
	out *sqs.ReceiveMessageOutput
	err error
}

func startReceive(client *sqs.Client, ctx context.Context, queue *string, wait int32) <-chan pollingResult {
	result := make(chan pollingResult, 1)
	go func() {
		out, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: queue, WaitTimeSeconds: wait, MaxNumberOfMessages: 10})
		result <- pollingResult{out, err}
	}()
	return result
}

func requirePending(t *testing.T, result <-chan pollingResult) {
	t.Helper()
	select {
	case got := <-result:
		t.Fatalf("receive returned prematurely: %+v %v", got.out, got.err)
	case <-time.After(150 * time.Millisecond):
	}
}

func awaitMessage(t *testing.T, result <-chan pollingResult, expected string) types.Message {
	t.Helper()
	select {
	case got := <-result:
		if got.err != nil || len(got.out.Messages) != 1 || aws.ToString(got.out.Messages[0].Body) != expected {
			t.Fatalf("receive: %+v %v", got.out, got.err)
		}
		return got.out.Messages[0]
	case <-time.After(3 * time.Second):
		t.Fatal("receive did not wake")
	}
	return types.Message{}
}

func TestPollingWakesOnSendAndBatchSend(t *testing.T) {
	t.Parallel()
	client := newSQSClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	queue := deliveryQueue(t, client, ctx, nil)
	for _, batch := range []bool{false, true} {
		pending := startReceive(client, ctx, queue, 20)
		requirePending(t, pending)
		var err error
		if batch {
			_, err = client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: queue, Entries: []types.SendMessageBatchRequestEntry{{Id: aws.String("a"), MessageBody: aws.String("arrived")}}})
		} else {
			_, err = client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: queue, MessageBody: aws.String("arrived")})
		}
		if err != nil {
			t.Fatal(err)
		}
		message := awaitMessage(t, pending, "arrived")
		deleteMessage(t, ctx, client, queue, message.ReceiptHandle)
	}
}

func TestPollingQuotaAndReleaseForStandardAndFIFO(t *testing.T) {
	t.Parallel()
	for _, fifo := range []bool{false, true} {
		t.Run(fmt.Sprint("FIFO=", fifo), func(t *testing.T) {
			client := newSQSClient(t)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			attrs := map[string]string{"LqsMaxInFlightMessages": "1"}
			if fifo {
				attrs["FifoQueue"] = "true"
				attrs["ContentBasedDeduplication"] = "true"
			}
			queue := deliveryQueue(t, client, ctx, attrs)
			for _, body := range []string{"one", "two"} {
				input := &sqs.SendMessageInput{QueueUrl: queue, MessageBody: aws.String(body)}
				if fifo {
					input.MessageGroupId = aws.String(body)
				}
				if _, err := client.SendMessage(ctx, input); err != nil {
					t.Fatal(err)
				}
			}
			first, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: queue, MaxNumberOfMessages: 10})
			if err != nil || len(first.Messages) != 1 {
				t.Fatalf("capacity: %+v %v", first, err)
			}
			if pollingAttributes(t, client, ctx, queue)["ApproximateNumberOfMessagesNotVisible"] != "1" {
				t.Fatal("in-flight count not reported")
			}
			out, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: queue})
			if fifo {
				if err != nil || len(out.Messages) != 0 {
					t.Fatalf("FIFO cap: %+v %v", out, err)
				}
			} else {
				requireHTTPError(t, err, "OverLimit")
			}
			start := time.Now()
			out, err = client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: queue, WaitTimeSeconds: 1})
			if err != nil || len(out.Messages) != 0 || time.Since(start) < time.Second {
				t.Fatalf("cap timeout: %+v %v (%v)", out, err, time.Since(start))
			}
			pending := startReceive(client, ctx, queue, 20)
			requirePending(t, pending)
			_, err = client.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{QueueUrl: queue, Entries: []types.DeleteMessageBatchRequestEntry{{Id: aws.String("release"), ReceiptHandle: first.Messages[0].ReceiptHandle}}})
			if err != nil {
				t.Fatal(err)
			}
			second := awaitMessage(t, pending, "two")
			pending = startReceive(client, ctx, queue, 20)
			requirePending(t, pending)
			_, err = client.ChangeMessageVisibilityBatch(ctx, &sqs.ChangeMessageVisibilityBatchInput{QueueUrl: queue, Entries: []types.ChangeMessageVisibilityBatchRequestEntry{{Id: aws.String("release"), ReceiptHandle: second.ReceiptHandle, VisibilityTimeout: 0}}})
			if err != nil {
				t.Fatal(err)
			}
			retried := awaitMessage(t, pending, "two")
			deleteMessage(t, ctx, client, queue, retried.ReceiptHandle)
			if pollingAttributes(t, client, ctx, queue)["ApproximateNumberOfMessagesNotVisible"] != "0" {
				t.Fatal("delete did not release slot")
			}
		})
	}
}

func TestPollingDelayAndVisibilityTimers(t *testing.T) {
	t.Parallel()
	client := newSQSClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	queue := deliveryQueue(t, client, ctx, map[string]string{"DelaySeconds": "1", "VisibilityTimeout": "1", "LqsMaxInFlightMessages": "1"})
	_, err := client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: queue, MessageBody: aws.String("timer")})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		start := time.Now()
		message := awaitMessage(t, startReceive(client, ctx, queue, 20), "timer")
		if time.Since(start) < 800*time.Millisecond {
			t.Fatalf("timer ignored: %v", time.Since(start))
		}
		if i == 1 {
			deleteMessage(t, ctx, client, queue, message.ReceiptHandle)
		}
	}
}

func TestQueryPollingAndWaitAttributes(t *testing.T) {
	t.Parallel()
	client := newSQSClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	queue := deliveryQueue(t, client, ctx, nil)
	postQuery(t, lqsEndpoint(), url.Values{"Action": {"SetQueueAttributes"}, "QueueUrl": {*queue}, "Attribute.1.Name": {"ReceiveMessageWaitTimeSeconds"}, "Attribute.1.Value": {"1"}})
	values := url.Values{"Action": {"ReceiveMessage"}, "QueueUrl": {*queue}}
	start := time.Now()
	body := postQuery(t, lqsEndpoint(), values)
	if time.Since(start) < time.Second {
		t.Fatal("Query default wait ignored")
	}
	var empty struct {
		Result struct {
			Messages []struct{ Body string } `xml:"Message"`
		} `xml:"ReceiveMessageResult"`
	}
	if err := xml.Unmarshal(body, &empty); err != nil || len(empty.Result.Messages) != 0 {
		t.Fatalf("Query timeout: %s %v", body, err)
	}
	// A Query request can stay pending while a JSON SDK request writes.
	done := make(chan error, 1)
	go func() {
		time.Sleep(200 * time.Millisecond)
		_, err := client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: queue, MessageBody: aws.String("query-arrival")})
		done <- err
	}()
	values.Set("WaitTimeSeconds", "20")
	start = time.Now()
	body = postQuery(t, lqsEndpoint(), values)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := xml.Unmarshal(body, &empty); err != nil || len(empty.Result.Messages) != 1 || empty.Result.Messages[0].Body != "query-arrival" {
		t.Fatalf("Query arrival: %s %v", body, err)
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("Query did not return promptly")
	}
}
