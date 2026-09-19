package integration_test

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
)

func TestAWSSDKV2FIFOEndToEnd(t *testing.T) {
	client := newSQSClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	queueName := fmt.Sprintf("orders-%d.fifo", time.Now().UnixNano())
	created, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
		Attributes: map[string]string{
			"FifoQueue":                 "true",
			"ContentBasedDeduplication": "true",
		},
	})
	if err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	if created.QueueUrl == nil || !strings.HasSuffix(*created.QueueUrl, "/"+queueName) {
		t.Fatalf("unexpected queue URL: %v", created.QueueUrl)
	}
	queueURL := created.QueueUrl

	send := func(body, group string) {
		t.Helper()
		output, err := client.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:       queueURL,
			MessageBody:    aws.String(body),
			MessageGroupId: aws.String(group),
		})
		if err != nil {
			t.Fatalf("SendMessage(%q): %v", body, err)
		}
		if output.MessageId == nil || output.MD5OfMessageBody == nil || output.SequenceNumber == nil {
			t.Fatalf("SendMessage(%q) returned incomplete metadata: %#v", body, output)
		}
	}

	send("a-1", "a")
	send("a-2", "a")
	send("b-1", "b")

	first, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 10,
	})
	if err != nil {
		t.Fatalf("ReceiveMessage(first): %v", err)
	}
	if len(first.Messages) != 2 {
		t.Fatalf("first receive returned %d messages, want 2: %#v", len(first.Messages), first.Messages)
	}
	a1 := findMessage(t, first.Messages, "a-1")
	b1 := findMessage(t, first.Messages, "b-1")

	_, err = client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          queueURL,
		ReceiptHandle:     b1.ReceiptHandle,
		VisibilityTimeout: 0,
	})
	if err != nil {
		t.Fatalf("ChangeMessageVisibility: %v", err)
	}

	retried, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 1,
	})
	if err != nil {
		t.Fatalf("ReceiveMessage(retry): %v", err)
	}
	if len(retried.Messages) != 1 || aws.ToString(retried.Messages[0].Body) != "b-1" {
		t.Fatalf("visibility retry returned unexpected messages: %#v", retried.Messages)
	}
	deleteMessage(t, ctx, client, queueURL, retried.Messages[0].ReceiptHandle)
	deleteMessage(t, ctx, client, queueURL, a1.ReceiptHandle)

	next, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 10,
	})
	if err != nil {
		t.Fatalf("ReceiveMessage(next): %v", err)
	}
	if len(next.Messages) != 1 || aws.ToString(next.Messages[0].Body) != "a-2" {
		t.Fatalf("next receive returned unexpected messages: %#v", next.Messages)
	}
	deleteMessage(t, ctx, client, queueURL, next.Messages[0].ReceiptHandle)
}

func TestAWSSDKV2ReceivesSQSCompatibleErrors(t *testing.T) {
	client := newSQSClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(fmt.Sprintf("invalid-%d.fifo", time.Now().UnixNano())),
	})
	if err == nil {
		t.Fatal("CreateQueue unexpectedly accepted a Standard queue with a .fifo suffix")
	}
	var apiError smithy.APIError
	if !errors.As(err, &apiError) {
		t.Fatalf("CreateQueue returned a non-Smithy error: %T: %v", err, err)
	}
	if apiError.ErrorCode() != "InvalidParameterValue" {
		t.Fatalf("error code = %q, want InvalidParameterValue: %v", apiError.ErrorCode(), err)
	}
}

func TestAWSSDKV2CreateQueueIsIdempotent(t *testing.T) {
	client := newSQSClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queueName := fmt.Sprintf("idempotent-%d", time.Now().UnixNano())
	input := &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
		Attributes: map[string]string{
			"VisibilityTimeout": "45",
		},
	}
	first, err := client.CreateQueue(ctx, input)
	if err != nil {
		t.Fatalf("first CreateQueue: %v", err)
	}
	second, err := client.CreateQueue(ctx, input)
	if err != nil {
		t.Fatalf("idempotent CreateQueue: %v", err)
	}
	if aws.ToString(first.QueueUrl) != aws.ToString(second.QueueUrl) {
		t.Fatalf("queue URLs differ: %q != %q", aws.ToString(first.QueueUrl), aws.ToString(second.QueueUrl))
	}

	_, err = client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
		Attributes: map[string]string{
			"VisibilityTimeout": "60",
		},
	})
	if err == nil {
		t.Fatal("CreateQueue unexpectedly accepted different attributes for an existing queue")
	}
	var apiError smithy.APIError
	if !errors.As(err, &apiError) || apiError.ErrorCode() != "QueueNameExists" {
		t.Fatalf("CreateQueue returned %T with code %q, want QueueNameExists: %v", err, apiErrorCode(err), err)
	}
}

func TestQueryProtocolOperations(t *testing.T) {
	endpoint := lqsEndpoint()
	queueName := fmt.Sprintf("query-%d", time.Now().UnixNano())
	createdBody := postQuery(t, endpoint, url.Values{
		"Action":    {"CreateQueue"},
		"Version":   {"2012-11-05"},
		"QueueName": {queueName},
	})
	var created struct {
		Result struct {
			QueueURL string `xml:"QueueUrl"`
		} `xml:"CreateQueueResult"`
	}
	if err := xml.Unmarshal(createdBody, &created); err != nil {
		t.Fatalf("decode Query CreateQueue: %v; body = %s", err, createdBody)
	}
	queueURL := endpoint + "/000000000000/" + queueName
	if created.Result.QueueURL != queueURL {
		t.Fatalf("Query queue URL = %q, want %q", created.Result.QueueURL, queueURL)
	}

	postQuery(t, endpoint, url.Values{
		"Action":      {"SendMessage"},
		"Version":     {"2012-11-05"},
		"QueueUrl":    {queueURL},
		"MessageBody": {"query-body<&>"},
	})

	receivedBody := postQuery(t, endpoint, url.Values{
		"Action":              {"ReceiveMessage"},
		"Version":             {"2012-11-05"},
		"QueueUrl":            {queueURL},
		"MaxNumberOfMessages": {"1"},
	})
	var received struct {
		Result struct {
			Messages []struct {
				Body          string `xml:"Body"`
				ReceiptHandle string `xml:"ReceiptHandle"`
			} `xml:"Message"`
		} `xml:"ReceiveMessageResult"`
	}
	if err := xml.Unmarshal(receivedBody, &received); err != nil {
		t.Fatalf("decode Query ReceiveMessage: %v; body = %s", err, receivedBody)
	}
	if len(received.Result.Messages) != 1 || received.Result.Messages[0].Body != "query-body<&>" {
		t.Fatalf("unexpected Query messages: %#v", received.Result.Messages)
	}
	receiptHandle := received.Result.Messages[0].ReceiptHandle

	postQuery(t, endpoint, url.Values{
		"Action":            {"ChangeMessageVisibility"},
		"Version":           {"2012-11-05"},
		"QueueUrl":          {queueURL},
		"ReceiptHandle":     {receiptHandle},
		"VisibilityTimeout": {"0"},
	})
	retriedBody := postQuery(t, endpoint, url.Values{
		"Action":              {"ReceiveMessage"},
		"Version":             {"2012-11-05"},
		"QueueUrl":            {queueURL},
		"MaxNumberOfMessages": {"1"},
	})
	received.Result.Messages = nil
	if err := xml.Unmarshal(retriedBody, &received); err != nil {
		t.Fatalf("decode retried Query ReceiveMessage: %v; body = %s", err, retriedBody)
	}
	if len(received.Result.Messages) != 1 || received.Result.Messages[0].Body != "query-body<&>" {
		t.Fatalf("unexpected retried Query messages: %#v", received.Result.Messages)
	}

	postQuery(t, endpoint, url.Values{
		"Action":        {"DeleteMessage"},
		"Version":       {"2012-11-05"},
		"QueueUrl":      {queueURL},
		"ReceiptHandle": {received.Result.Messages[0].ReceiptHandle},
	})
}

func newSQSClient(t *testing.T) *sqs.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load AWS config: %v", err)
	}
	return sqs.NewFromConfig(cfg, func(options *sqs.Options) {
		options.BaseEndpoint = aws.String(lqsEndpoint())
		options.RetryMaxAttempts = 1
	})
}

func lqsEndpoint() string {
	if endpoint := os.Getenv("LQS_ENDPOINT"); endpoint != "" {
		return strings.TrimRight(endpoint, "/")
	}
	return "http://127.0.0.1:9324"
}

func findMessage(t *testing.T, messages []types.Message, body string) types.Message {
	t.Helper()
	for _, message := range messages {
		if aws.ToString(message.Body) == body {
			return message
		}
	}
	t.Fatalf("message %q not found in %#v", body, messages)
	return types.Message{}
}

func apiErrorCode(err error) string {
	var apiError smithy.APIError
	if errors.As(err, &apiError) {
		return apiError.ErrorCode()
	}
	return ""
}

func postQuery(t *testing.T, endpoint string, values url.Values) []byte {
	t.Helper()
	response, err := http.PostForm(endpoint+"/", values)
	if err != nil {
		t.Fatalf("Query %s: %v", values.Get("Action"), err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read Query %s response: %v", values.Get("Action"), err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("Query %s status = %d, body = %s", values.Get("Action"), response.StatusCode, body)
	}
	if response.Header.Get("x-amzn-requestid") == "" {
		t.Fatalf("Query %s response did not include x-amzn-requestid", values.Get("Action"))
	}
	return body
}

func deleteMessage(
	t *testing.T,
	ctx context.Context,
	client *sqs.Client,
	queueURL *string,
	receiptHandle *string,
) {
	t.Helper()
	if receiptHandle == nil {
		t.Fatal("message has no receipt handle")
	}
	if _, err := client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      queueURL,
		ReceiptHandle: receiptHandle,
	}); err != nil {
		t.Fatalf("DeleteMessage: %v", err)
	}
}
