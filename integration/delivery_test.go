package integration_test

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func deliveryQueue(t *testing.T, client *sqs.Client, ctx context.Context, attributes map[string]string) *string {
	t.Helper()
	name := fmt.Sprintf("delivery-%d", time.Now().UnixNano())
	if attributes["FifoQueue"] == "true" {
		name += ".fifo"
	}
	out, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name), Attributes: attributes})
	if err != nil {
		t.Fatal(err)
	}
	again, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name), Attributes: attributes})
	if err != nil || aws.ToString(again.QueueUrl) != aws.ToString(out.QueueUrl) {
		t.Fatalf("idempotent create: %v", err)
	}
	return out.QueueUrl
}

func TestDeliveryAttributesAndSizeLimits(t *testing.T) {
	client := newSQSClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	queueURL := deliveryQueue(t, client, ctx, map[string]string{"DelaySeconds": "1", "MessageRetentionPeriod": "60", "MaximumMessageSize": "1024"})
	get := func() map[string]string {
		t.Helper()
		out, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: queueURL, AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameAll}})
		if err != nil {
			t.Fatal(err)
		}
		return out.Attributes
	}
	got := get()
	if got["DelaySeconds"] != "1" || got["MessageRetentionPeriod"] != "60" || got["MaximumMessageSize"] != "1024" {
		t.Fatalf("attributes: %v", got)
	}
	for _, invalid := range []map[string]string{
		{"DelaySeconds": "901"}, {"DelaySeconds": "-1"}, {"DelaySeconds": "1.5"},
		{"MessageRetentionPeriod": "59"}, {"MessageRetentionPeriod": "1209601"},
		{"MaximumMessageSize": "1023"}, {"MaximumMessageSize": "1048577"},
		{"DelaySeconds": "0", "MaximumMessageSize": "0"},
	} {
		_, err := client.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{QueueUrl: queueURL, Attributes: invalid})
		requireHTTPError(t, err, "InvalidParameterValue")
		if get()["DelaySeconds"] != "1" {
			t.Fatal("invalid attribute update partially applied")
		}
		_, err = client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(fmt.Sprintf("invalid-%d", time.Now().UnixNano())), Attributes: invalid})
		requireHTTPError(t, err, "InvalidParameterValue")
	}
	send := func(body string) error {
		_, err := client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: queueURL, MessageBody: aws.String(body)})
		return err
	}
	if err := send(strings.Repeat("é", 512)); err != nil {
		t.Fatal(err)
	}
	requireHTTPError(t, send(strings.Repeat("é", 513)), "InvalidParameterValue")
	requireHTTPError(t, send(""), "InvalidParameterValue")
	_, err := client.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{QueueUrl: queueURL, Attributes: map[string]string{"DelaySeconds": "0", "MaximumMessageSize": "1048576"}})
	if err != nil {
		t.Fatal(err)
	}
	if get()["MaximumMessageSize"] != "1048576" || get()["DelaySeconds"] != "0" {
		t.Fatal("attributes not updated")
	}
	for _, body := range []string{"x", strings.Repeat("a", 1<<20), strings.Repeat("\t", 1<<20)} {
		if err := send(body); err != nil {
			t.Fatalf("valid %d-byte body rejected: %v", len(body), err)
		}
	}
	requireHTTPError(t, send(strings.Repeat("a", (1<<20)+1)), "InvalidParameterValue")
	// Query encoding expands this valid body to 3 MiB; limits apply after decoding.
	postQuery(t, lqsEndpoint(), url.Values{"Action": {"SendMessage"}, "QueueUrl": {*queueURL}, "MessageBody": {strings.Repeat("\t", 1<<20)}})
}

func TestDeliveryDelayThroughSDKAndQuery(t *testing.T) {
	client := newSQSClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	standard := deliveryQueue(t, client, ctx, map[string]string{"DelaySeconds": "900"})
	fifo := deliveryQueue(t, client, ctx, map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "true", "DelaySeconds": "2"})
	_, err := client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: standard, MessageBody: aws.String("standard"), DelaySeconds: 2})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: fifo, MessageBody: aws.String("fifo"), MessageGroupId: aws.String("a")})
	if err != nil {
		t.Fatal(err)
	}
	for _, queueURL := range []*string{standard, fifo} {
		out, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: queueURL})
		if err != nil || len(out.Messages) != 0 {
			t.Fatalf("delay not respected: %+v %v", out, err)
		}
	}
	_, err = client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: fifo, MessageBody: aws.String("invalid"), MessageGroupId: aws.String("b"), DelaySeconds: 1})
	requireHTTPError(t, err, "InvalidParameterValue")
	_, err = client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: standard, MessageBody: aws.String("invalid"), DelaySeconds: 901})
	requireHTTPError(t, err, "InvalidParameterValue")
	postQuery(t, lqsEndpoint(), url.Values{"Action": {"SendMessage"}, "QueueUrl": {*standard}, "MessageBody": {"override-zero"}, "DelaySeconds": {"0"}})
	immediate, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: standard})
	if err != nil || len(immediate.Messages) != 1 || aws.ToString(immediate.Messages[0].Body) != "override-zero" {
		t.Fatalf("zero override: %+v %v", immediate, err)
	}
	time.Sleep(2100 * time.Millisecond)
	for _, queueURL := range []*string{standard, fifo} {
		out, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: queueURL})
		if err != nil || len(out.Messages) != 1 {
			t.Fatalf("delayed receive: %+v %v", out, err)
		}
	}
}

func TestRetentionExpiresThroughSDK(t *testing.T) {
	t.Parallel()
	client := newSQSClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Second)
	defer cancel()
	queueURL := deliveryQueue(t, client, ctx, map[string]string{"MessageRetentionPeriod": "60"})
	_, err := client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: queueURL, MessageBody: aws.String("inflight")})
	if err != nil {
		t.Fatal(err)
	}
	inflight, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: queueURL})
	if err != nil || len(inflight.Messages) != 1 {
		t.Fatalf("initial receive: %+v %v", inflight, err)
	}
	_, err = client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{QueueUrl: queueURL, ReceiptHandle: inflight.Messages[0].ReceiptHandle, VisibilityTimeout: 120})
	if err != nil {
		t.Fatal(err)
	}
	for _, delay := range []int32{0, 120} {
		_, err := client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: queueURL, MessageBody: aws.String(fmt.Sprintf("expires-%d", delay)), DelaySeconds: delay})
		if err != nil {
			t.Fatal(err)
		}
	}
	// SQS's minimum retention is 60 seconds; exercise the real server clock.
	timer := time.NewTimer(61 * time.Second)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	out, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: queueURL, MaxNumberOfMessages: 10})
	if err != nil || len(out.Messages) != 0 {
		t.Fatalf("expired messages delivered: %+v %v", out, err)
	}
	_, err = client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{QueueUrl: queueURL, ReceiptHandle: inflight.Messages[0].ReceiptHandle, VisibilityTimeout: 0})
	requireHTTPError(t, err, "ReceiptHandleIsInvalid")
	_, err = client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: queueURL, MessageBody: aws.String("fresh")})
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: queueURL, MaxNumberOfMessages: 10})
	if err != nil || len(fresh.Messages) != 1 || aws.ToString(fresh.Messages[0].Body) != "fresh" {
		t.Fatalf("fresh receive: %+v %v", fresh, err)
	}
}
