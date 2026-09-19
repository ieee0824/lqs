package integration_test

import (
	"context"
	"crypto/md5"
	"encoding/xml"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func checkBatchFailures(t *testing.T, failures []types.BatchResultErrorEntry, expected map[string]string) {
	t.Helper()
	if len(failures) != len(expected) {
		t.Fatalf("failures: %+v; expected %v", failures, expected)
	}
	for _, failure := range failures {
		if expected[aws.ToString(failure.Id)] != aws.ToString(failure.Code) || !failure.SenderFault || aws.ToString(failure.Message) == "" {
			t.Fatalf("unexpected failure: %+v", failure)
		}
	}
}

func TestSDKBatchPartialSuccess(t *testing.T) {
	client := newSQSClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	queue := deliveryQueue(t, client, ctx, map[string]string{"MaximumMessageSize": "1024"})
	sent, err := client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: queue, Entries: []types.SendMessageBatchRequestEntry{
		{Id: aws.String("first"), MessageBody: aws.String("one<&>")},
		{Id: aws.String("large"), MessageBody: aws.String(strings.Repeat("é", 513))},
		{Id: aws.String("empty"), MessageBody: aws.String("")},
		{Id: aws.String("second"), MessageBody: aws.String("two")},
		{Id: aws.String("delay"), MessageBody: aws.String("bad"), DelaySeconds: 901},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(sent.Successful) != 2 {
		t.Fatalf("send: %+v", sent)
	}
	checkBatchFailures(t, sent.Failed, map[string]string{"large": "InvalidParameterValue", "empty": "InvalidParameterValue", "delay": "InvalidParameterValue"})
	for i, body := range []string{"one<&>", "two"} {
		if aws.ToString(sent.Successful[i].MD5OfMessageBody) != fmt.Sprintf("%x", md5.Sum([]byte(body))) || sent.Successful[i].MessageId == nil || sent.Successful[i].SequenceNumber != nil {
			t.Fatalf("metadata: %+v", sent.Successful[i])
		}
	}
	received, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: queue, MaxNumberOfMessages: 10})
	if err != nil || len(received.Messages) != 2 {
		t.Fatalf("receive: %+v %v", received, err)
	}
	one := findMessage(t, received.Messages, "one<&>")
	two := findMessage(t, received.Messages, "two")
	changed, err := client.ChangeMessageVisibilityBatch(ctx, &sqs.ChangeMessageVisibilityBatchInput{QueueUrl: queue, Entries: []types.ChangeMessageVisibilityBatchRequestEntry{
		{Id: aws.String("release"), ReceiptHandle: one.ReceiptHandle, VisibilityTimeout: 0},
		{Id: aws.String("bad"), ReceiptHandle: aws.String("invalid<&>")},
		{Id: aws.String("range"), ReceiptHandle: two.ReceiptHandle, VisibilityTimeout: 43201},
	}})
	if err != nil || len(changed.Successful) != 1 {
		t.Fatalf("change: %+v %v", changed, err)
	}
	checkBatchFailures(t, changed.Failed, map[string]string{"bad": "ReceiptHandleIsInvalid", "range": "InvalidParameterValue"})
	retried, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: queue, MaxNumberOfMessages: 10})
	if err != nil || len(retried.Messages) != 1 || aws.ToString(retried.Messages[0].Body) != "one<&>" {
		t.Fatalf("retry: %+v %v", retried, err)
	}
	deleted, err := client.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{QueueUrl: queue, Entries: []types.DeleteMessageBatchRequestEntry{
		{Id: aws.String("stale"), ReceiptHandle: one.ReceiptHandle},
		{Id: aws.String("first"), ReceiptHandle: retried.Messages[0].ReceiptHandle},
		{Id: aws.String("bad"), ReceiptHandle: aws.String("invalid")},
		{Id: aws.String("second"), ReceiptHandle: two.ReceiptHandle},
	}})
	if err != nil || len(deleted.Successful) != 2 {
		t.Fatalf("delete: %+v %v", deleted, err)
	}
	checkBatchFailures(t, deleted.Failed, map[string]string{"stale": "ReceiptHandleIsInvalid", "bad": "ReceiptHandleIsInvalid"})
	empty, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: queue, MaxNumberOfMessages: 10})
	if err != nil || len(empty.Messages) != 0 {
		t.Fatalf("after delete: %+v %v", empty, err)
	}
}

func TestSDKBatchEnvelopeValidation(t *testing.T) {
	client := newSQSClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	queue := deliveryQueue(t, client, ctx, nil)
	for _, test := range []struct {
		ids  []string
		code string
	}{
		{[]string{}, "EmptyBatchRequest"}, {[]string{"same", "same"}, "BatchEntryIdsNotDistinct"},
		{[]string{"valid", "bad id"}, "InvalidBatchEntryId"}, {[]string{strings.Repeat("a", 81)}, "InvalidBatchEntryId"},
		{[]string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11"}, "TooManyEntriesInBatchRequest"},
	} {
		sends := []types.SendMessageBatchRequestEntry{}
		deletes := []types.DeleteMessageBatchRequestEntry{}
		changes := []types.ChangeMessageVisibilityBatchRequestEntry{}
		for _, id := range test.ids {
			sends = append(sends, types.SendMessageBatchRequestEntry{Id: aws.String(id), MessageBody: aws.String("body")})
			deletes = append(deletes, types.DeleteMessageBatchRequestEntry{Id: aws.String(id), ReceiptHandle: aws.String("handle")})
			changes = append(changes, types.ChangeMessageVisibilityBatchRequestEntry{Id: aws.String(id), ReceiptHandle: aws.String("handle")})
		}
		_, err := client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: queue, Entries: sends})
		requireHTTPError(t, err, test.code)
		_, err = client.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{QueueUrl: queue, Entries: deletes})
		requireHTTPError(t, err, test.code)
		_, err = client.ChangeMessageVisibilityBatch(ctx, &sqs.ChangeMessageVisibilityBatchInput{QueueUrl: queue, Entries: changes})
		requireHTTPError(t, err, test.code)
	}
	_, err := client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: queue, Entries: []types.SendMessageBatchRequestEntry{
		{Id: aws.String("a"), MessageBody: aws.String(strings.Repeat("a", 1<<19))}, {Id: aws.String("b"), MessageBody: aws.String(strings.Repeat("b", (1<<19)+1))},
	}})
	requireHTTPError(t, err, "BatchRequestTooLong")
	empty, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: queue, MaxNumberOfMessages: 10})
	if err != nil || len(empty.Messages) != 0 {
		t.Fatalf("invalid envelope wrote messages: %+v %v", empty, err)
	}
	// An exactly 1 MiB batch remains valid even when JSON escaping doubles its wire size.
	sent, err := client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: queue, Entries: []types.SendMessageBatchRequestEntry{
		{Id: aws.String(strings.Repeat("a", 80)), MessageBody: aws.String(strings.Repeat("\t", 1<<19))}, {Id: aws.String("b"), MessageBody: aws.String(strings.Repeat("é", 1<<18))},
	}})
	if err != nil || len(sent.Successful) != 2 || len(sent.Failed) != 0 {
		t.Fatalf("boundary: %+v %v", sent, err)
	}
}

func TestSDKBatchFIFOOrderAndDeduplication(t *testing.T) {
	client := newSQSClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	queue := deliveryQueue(t, client, ctx, map[string]string{"FifoQueue": "true"})
	makeEntry := func(id, body, key string) types.SendMessageBatchRequestEntry {
		return types.SendMessageBatchRequestEntry{Id: aws.String(id), MessageBody: aws.String(body), MessageGroupId: aws.String("g"), MessageDeduplicationId: aws.String(key)}
	}
	missingGroup := makeEntry("group", "bad", "unused")
	missingGroup.MessageGroupId = nil
	missingKey := makeEntry("key", "bad", "unused")
	missingKey.MessageDeduplicationId = nil
	sent, err := client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: queue, Entries: []types.SendMessageBatchRequestEntry{
		makeEntry("first", "one", "one"), missingGroup, makeEntry("dup", "duplicate", "one"), missingKey, makeEntry("second", "two", "two"), makeEntry("badkey", "bad", ""),
	}})
	if err != nil || len(sent.Successful) != 3 {
		t.Fatalf("fifo send: %+v %v", sent, err)
	}
	checkBatchFailures(t, sent.Failed, map[string]string{"group": "InvalidParameterValue", "key": "InvalidParameterValue", "badkey": "InvalidParameterValue"})
	if aws.ToString(sent.Successful[0].MessageId) != aws.ToString(sent.Successful[1].MessageId) || aws.ToString(sent.Successful[0].SequenceNumber) == "" || aws.ToString(sent.Successful[0].SequenceNumber) != aws.ToString(sent.Successful[1].SequenceNumber) {
		t.Fatalf("dedup metadata: %+v", sent)
	}
	_, err = client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: queue, MessageBody: aws.String("retry"), MessageGroupId: aws.String("g"), MessageDeduplicationId: aws.String("two")})
	if err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{"one", "two"} {
		out, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: queue, MaxNumberOfMessages: 10})
		if err != nil || len(out.Messages) != 1 || aws.ToString(out.Messages[0].Body) != body {
			t.Fatalf("FIFO order: %+v %v", out, err)
		}
		deleteMessage(t, ctx, client, queue, out.Messages[0].ReceiptHandle)
	}
	empty, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: queue})
	if err != nil || len(empty.Messages) != 0 {
		t.Fatalf("duplicate delivered: %+v %v", empty, err)
	}
}

type queryBatchEntry struct {
	ID          string `xml:"Id"`
	MessageID   string `xml:"MessageId"`
	MD5         string `xml:"MD5OfMessageBody"`
	Code        string `xml:"Code"`
	Message     string `xml:"Message"`
	SenderFault bool   `xml:"SenderFault"`
}

func queryBatch(t *testing.T, action string, queue *string, entries []map[string]string) ([]queryBatchEntry, []queryBatchEntry) {
	t.Helper()
	values := url.Values{"Action": {action}, "QueueUrl": {*queue}}
	for i, entry := range entries {
		for name, value := range entry {
			values.Set(fmt.Sprintf("%sRequestEntry.%d.%s", action, i+1, name), value)
		}
	}
	body := postQuery(t, lqsEndpoint(), values)
	var out struct {
		Result struct {
			Sent    []queryBatchEntry `xml:"SendMessageBatchResultEntry"`
			Deleted []queryBatchEntry `xml:"DeleteMessageBatchResultEntry"`
			Changed []queryBatchEntry `xml:"ChangeMessageVisibilityBatchResultEntry"`
			Failed  []queryBatchEntry `xml:"BatchResultErrorEntry"`
		} `xml:",any"`
	}
	// Decode the result element without treating ResponseMetadata as another result.
	decoder := xml.NewDecoder(strings.NewReader(string(body)))
	for {
		x, err := decoder.Token()
		if err != nil {
			t.Fatal(err)
		}
		if start, ok := x.(xml.StartElement); ok && start.Name.Local == action+"Result" {
			if err := decoder.DecodeElement(&out.Result, &start); err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	success := append(append(out.Result.Sent, out.Result.Deleted...), out.Result.Changed...)
	return success, out.Result.Failed
}

func TestQueryBatchOperationsAndNumericOrder(t *testing.T) {
	client := newSQSClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	queue := deliveryQueue(t, client, ctx, map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "true"})
	entries := []map[string]string{}
	for i := 1; i <= 10; i++ {
		entries = append(entries, map[string]string{"Id": fmt.Sprint(i), "MessageBody": fmt.Sprintf("body-%d<&>", i), "MessageGroupId": "g"})
	}
	entries[4]["MessageGroupId"] = ""
	success, failed := queryBatch(t, "SendMessageBatch", queue, entries)
	if len(success) != 9 || len(failed) != 1 || failed[0].ID != "5" || !failed[0].SenderFault {
		t.Fatalf("query send: %+v %+v", success, failed)
	}
	for i := 1; i <= 10; i++ {
		if i == 5 {
			continue
		}
		out, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: queue, MaxNumberOfMessages: 10})
		if err != nil || len(out.Messages) != 1 || aws.ToString(out.Messages[0].Body) != fmt.Sprintf("body-%d<&>", i) {
			t.Fatalf("query FIFO order: %+v %v", out, err)
		}
		handle := aws.ToString(out.Messages[0].ReceiptHandle)
		if i == 1 {
			success, failed = queryBatch(t, "ChangeMessageVisibilityBatch", queue, []map[string]string{{"Id": "ok", "ReceiptHandle": handle, "VisibilityTimeout": "0"}, {"Id": "bad", "ReceiptHandle": "invalid<&>", "VisibilityTimeout": "0"}, {"Id": "range", "ReceiptHandle": handle, "VisibilityTimeout": "-1"}})
			if len(success) != 1 || len(failed) != 2 || failed[0].Code != "ReceiptHandleIsInvalid" || !strings.Contains(failed[0].Message, "invalid<&>") || !failed[0].SenderFault {
				t.Fatalf("query visibility: %+v %+v", success, failed)
			}
			out, err = client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: queue})
			if err != nil || len(out.Messages) != 1 {
				t.Fatalf("query release: %+v %v", out, err)
			}
			handle = aws.ToString(out.Messages[0].ReceiptHandle)
		}
		success, failed = queryBatch(t, "DeleteMessageBatch", queue, []map[string]string{{"Id": "bad", "ReceiptHandle": "invalid<&>"}, {"Id": "ok", "ReceiptHandle": handle}})
		if len(success) != 1 || success[0].ID != "ok" || len(failed) != 1 || failed[0].Code != "ReceiptHandleIsInvalid" || !failed[0].SenderFault {
			t.Fatalf("query delete: %+v %+v", success, failed)
		}
	}
}
