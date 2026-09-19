package integration_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"encoding/xml"
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func sampleAttributes() map[string]types.MessageAttributeValue {
	return map[string]types.MessageAttributeValue{
		"text.label": {DataType: aws.String("String.custom"), StringValue: aws.String("日本語<&>")},
		"number":     {DataType: aws.String("Number.int"), StringValue: aws.String("12345678901234567890123456789012345678")},
		"binary":     {DataType: aws.String("Binary.image"), BinaryValue: []byte{0, 255, 1, 128}},
	}
}

// Independent implementation of the documented SQS attribute checksum wire encoding.
func attributeDigest(attributes map[string]types.MessageAttributeValue) string {
	var buffer bytes.Buffer
	part := func(value []byte) {
		_ = binary.Write(&buffer, binary.BigEndian, uint32(len(value)))
		buffer.Write(value)
	}
	names := make([]string, 0, len(attributes))
	for name := range attributes {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		value := attributes[name]
		part([]byte(name))
		part([]byte(aws.ToString(value.DataType)))
		if strings.HasPrefix(aws.ToString(value.DataType), "Binary") {
			buffer.WriteByte(2)
			part(value.BinaryValue)
		} else {
			buffer.WriteByte(1)
			part([]byte(aws.ToString(value.StringValue)))
		}
	}
	return fmt.Sprintf("%x", md5.Sum(buffer.Bytes()))
}

func TestAttributeRoundTripSelectionAndFIFOContentDedup(t *testing.T) {
	client := newSQSClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	queue := deliveryQueue(t, client, ctx, map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "true"})
	attrs := sampleAttributes()
	before := time.Now().UnixMilli()
	sent, err := client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: queue, MessageBody: aws.String("body"), MessageGroupId: aws.String("g"), MessageAttributes: attrs})
	if err != nil {
		t.Fatal(err)
	}
	if aws.ToString(sent.MD5OfMessageAttributes) != attributeDigest(attrs) {
		t.Fatalf("send digest: %+v", sent)
	}
	changed := sampleAttributes()
	changed["text.label"] = types.MessageAttributeValue{DataType: aws.String("String.other"), StringValue: aws.String("not stored")}
	duplicate, err := client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: queue, MessageBody: aws.String("body"), MessageGroupId: aws.String("g"), MessageAttributes: changed})
	if err != nil || aws.ToString(duplicate.MessageId) != aws.ToString(sent.MessageId) || aws.ToString(duplicate.MD5OfMessageAttributes) != attributeDigest(changed) {
		t.Fatalf("dedup: %+v %v", duplicate, err)
	}
	receive := func(names []string, system []types.MessageSystemAttributeName) types.Message {
		t.Helper()
		out, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: queue, MessageAttributeNames: names, MessageSystemAttributeNames: system})
		if err != nil || len(out.Messages) != 1 {
			t.Fatalf("receive: %+v %v", out, err)
		}
		return out.Messages[0]
	}
	release := func(message types.Message) {
		t.Helper()
		_, err := client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{QueueUrl: queue, ReceiptHandle: message.ReceiptHandle, VisibilityTimeout: 0})
		if err != nil {
			t.Fatal(err)
		}
	}
	first := receive([]string{"All"}, []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll})
	if !reflect.DeepEqual(first.MessageAttributes, attrs) || aws.ToString(first.MD5OfMessageAttributes) != attributeDigest(attrs) {
		t.Fatalf("attribute round trip: %+v", first)
	}
	if first.Attributes["ApproximateReceiveCount"] != "1" || first.Attributes["MessageGroupId"] != "g" || first.Attributes["SequenceNumber"] != aws.ToString(sent.SequenceNumber) || first.Attributes["MessageDeduplicationId"] == "" || first.Attributes["SenderId"] != "000000000000" {
		t.Fatalf("system attrs: %+v", first.Attributes)
	}
	for _, name := range []string{"SentTimestamp", "ApproximateFirstReceiveTimestamp"} {
		timestamp, err := strconv.ParseInt(first.Attributes[name], 10, 64)
		if err != nil || timestamp < before || timestamp > time.Now().UnixMilli() {
			t.Fatalf("%s: %v %v", name, timestamp, err)
		}
	}
	release(first)
	selected := receive([]string{"text.*", "missing"}, []types.MessageSystemAttributeName{types.MessageSystemAttributeNameApproximateReceiveCount, types.MessageSystemAttributeNameApproximateFirstReceiveTimestamp})
	if len(selected.MessageAttributes) != 1 || !reflect.DeepEqual(selected.MessageAttributes["text.label"], attrs["text.label"]) || aws.ToString(selected.MD5OfMessageAttributes) != attributeDigest(selected.MessageAttributes) {
		t.Fatalf("selection: %+v", selected)
	}
	if len(selected.Attributes) != 2 || selected.Attributes["ApproximateReceiveCount"] != "2" || selected.Attributes["ApproximateFirstReceiveTimestamp"] != first.Attributes["ApproximateFirstReceiveTimestamp"] {
		t.Fatalf("retry system attrs: %+v", selected.Attributes)
	}
	release(selected)
	none := receive(nil, nil)
	if len(none.MessageAttributes) != 0 || len(none.Attributes) != 0 || none.MD5OfMessageAttributes != nil {
		t.Fatalf("unrequested attributes leaked: %+v", none)
	}
	release(none)
	legacy, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: queue, MessageAttributeNames: []string{".*"}, AttributeNames: []types.QueueAttributeName{types.QueueAttributeName("SentTimestamp")}})
	if err != nil || len(legacy.Messages) != 1 || len(legacy.Messages[0].Attributes) != 1 || !reflect.DeepEqual(legacy.Messages[0].MessageAttributes, attrs) {
		t.Fatalf("legacy selection: %+v %v", legacy, err)
	}
	deleteMessage(t, ctx, client, queue, legacy.Messages[0].ReceiptHandle)
	empty, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: queue})
	if err != nil || len(empty.Messages) != 0 {
		t.Fatalf("duplicate delivered: %+v %v", empty, err)
	}
}

func TestAttributeValidationAndSizeAccounting(t *testing.T) {
	client := newSQSClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	queue := deliveryQueue(t, client, ctx, map[string]string{"MaximumMessageSize": "1024"})
	for _, attrs := range []map[string]types.MessageAttributeValue{
		{"AWS.bad": {DataType: aws.String("String"), StringValue: aws.String("v")}},
		{"bad..name": {DataType: aws.String("String"), StringValue: aws.String("v")}},
		{"n": {DataType: aws.String("Number"), StringValue: aws.String("NaN")}},
		{"n": {DataType: aws.String("Number"), StringValue: aws.String("1e127")}},
		{"b": {DataType: aws.String("Binary"), StringValue: aws.String("wrong")}},
		{"s": {DataType: aws.String("String"), StringValue: aws.String("")}},
	} {
		_, err := client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: queue, MessageBody: aws.String("body"), MessageAttributes: attrs})
		requireHTTPError(t, err, "InvalidParameterValue")
	}
	attrs := map[string]types.MessageAttributeValue{"b": {DataType: aws.String("Binary.custom"), BinaryValue: []byte{0, 255}}}
	overhead := len("b") + len("Binary.custom") + 2
	_, err := client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: queue, MessageBody: aws.String(strings.Repeat("x", 1024-overhead+1)), MessageAttributes: attrs})
	requireHTTPError(t, err, "InvalidParameterValue")
	sent, err := client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: queue, Entries: []types.SendMessageBatchRequestEntry{
		{Id: aws.String("good"), MessageBody: aws.String(strings.Repeat("x", 1024-overhead)), MessageAttributes: attrs},
		{Id: aws.String("bad"), MessageBody: aws.String("x"), MessageAttributes: map[string]types.MessageAttributeValue{"n": {DataType: aws.String("Number"), StringValue: aws.String("no")}}},
	}})
	if err != nil || len(sent.Successful) != 1 || len(sent.Failed) != 1 || aws.ToString(sent.Successful[0].MD5OfMessageAttributes) != attributeDigest(attrs) {
		t.Fatalf("batch attributes: %+v %v", sent, err)
	}
	largeQueue := deliveryQueue(t, client, ctx, nil)
	_, err = client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: largeQueue, Entries: []types.SendMessageBatchRequestEntry{
		{Id: aws.String("a"), MessageBody: aws.String(strings.Repeat("a", 1<<19)), MessageAttributes: attrs},
		{Id: aws.String("b"), MessageBody: aws.String(strings.Repeat("b", 1<<19)), MessageAttributes: attrs},
	}})
	requireHTTPError(t, err, "BatchRequestTooLong")
	empty, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: largeQueue})
	if err != nil || len(empty.Messages) != 0 {
		t.Fatalf("oversized batch wrote data: %+v %v", empty, err)
	}
}

func TestQueryMessageAttributesAndSystemSelection(t *testing.T) {
	client := newSQSClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	queue := deliveryQueue(t, client, ctx, nil)
	attrs := map[string]types.MessageAttributeValue{
		"foo.text": {DataType: aws.String("String.<custom>"), StringValue: aws.String("value<&>日本語")},
		"foo.bin":  {DataType: aws.String("Binary.bytes"), BinaryValue: []byte{0, 128, 255}},
	}
	values := url.Values{"Action": {"SendMessage"}, "QueueUrl": {*queue}, "MessageBody": {"query"}}
	i := 1
	for name, value := range attrs {
		prefix := fmt.Sprintf("MessageAttribute.%d.", i)
		i++
		values.Set(prefix+"Name", name)
		values.Set(prefix+"Value.DataType", aws.ToString(value.DataType))
		if value.BinaryValue != nil {
			values.Set(prefix+"Value.BinaryValue", base64.StdEncoding.EncodeToString(value.BinaryValue))
		} else {
			values.Set(prefix+"Value.StringValue", aws.ToString(value.StringValue))
		}
	}
	body := postQuery(t, lqsEndpoint(), values)
	var sent struct {
		Result struct {
			Digest string `xml:"MD5OfMessageAttributes"`
		} `xml:"SendMessageResult"`
	}
	if err := xml.Unmarshal(body, &sent); err != nil || sent.Result.Digest != attributeDigest(attrs) {
		t.Fatalf("Query send: %s %v", body, err)
	}
	body = postQuery(t, lqsEndpoint(), url.Values{"Action": {"ReceiveMessage"}, "QueueUrl": {*queue}, "MessageAttributeName.1": {"foo.*"}, "MessageSystemAttributeName.1": {"SentTimestamp"}})
	var received struct {
		Result struct {
			Messages []struct {
				Digest     string `xml:"MD5OfMessageAttributes"`
				Attributes []struct {
					Name  string
					Value struct {
						DataType    string
						StringValue string
						BinaryValue string
					}
				} `xml:"MessageAttribute"`
				System []struct {
					Name  string
					Value string
				} `xml:"Attribute"`
			} `xml:"Message"`
		} `xml:"ReceiveMessageResult"`
	}
	if err := xml.Unmarshal(body, &received); err != nil || len(received.Result.Messages) != 1 {
		t.Fatalf("Query receive: %s %v", body, err)
	}
	message := received.Result.Messages[0]
	if message.Digest != attributeDigest(attrs) || len(message.Attributes) != 2 || len(message.System) != 1 || message.System[0].Name != "SentTimestamp" {
		t.Fatalf("Query metadata: %s", body)
	}
	for _, attribute := range message.Attributes {
		expected := attrs[attribute.Name]
		if attribute.Value.DataType != aws.ToString(expected.DataType) || attribute.Value.StringValue != aws.ToString(expected.StringValue) || attribute.Value.BinaryValue != base64.StdEncoding.EncodeToString(expected.BinaryValue) {
			t.Fatalf("Query attribute changed: %+v", attribute)
		}
	}
	// Query batch uses the same nested attribute wire format.
	postQuery(t, lqsEndpoint(), url.Values{"Action": {"SendMessageBatch"}, "QueueUrl": {*queue}, "SendMessageBatchRequestEntry.1.Id": {"a"}, "SendMessageBatchRequestEntry.1.MessageBody": {"batch"}, "SendMessageBatchRequestEntry.1.MessageAttribute.1.Name": {"n"}, "SendMessageBatchRequestEntry.1.MessageAttribute.1.Value.DataType": {"Number.custom"}, "SendMessageBatchRequestEntry.1.MessageAttribute.1.Value.StringValue": {"42"}})
	out, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: queue, MessageAttributeNames: []string{"All"}})
	if err != nil || len(out.Messages) != 1 || aws.ToString(out.Messages[0].MessageAttributes["n"].DataType) != "Number.custom" || aws.ToString(out.Messages[0].MessageAttributes["n"].StringValue) != "42" {
		t.Fatalf("Query batch: %+v %v", out, err)
	}
}
