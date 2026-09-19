package integration_test

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func TestManagementDiscoveryAndTags(t *testing.T) {
	t.Parallel()
	client := newSQSClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("management-%d-", time.Now().UnixNano())
	var queues []*string
	for _, suffix := range []string{"a", "b", "c"} {
		out, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(prefix + suffix), Tags: map[string]string{"環境": "開発", "empty": ""}})
		if err != nil {
			t.Fatal(err)
		}
		queues = append(queues, out.QueueUrl)
	}
	first, err := client.ListQueues(ctx, &sqs.ListQueuesInput{QueueNamePrefix: aws.String(prefix), MaxResults: aws.Int32(2)})
	if err != nil || len(first.QueueUrls) != 2 || first.NextToken == nil {
		t.Fatalf("first page: %+v %v", first, err)
	}
	next, err := client.ListQueues(ctx, &sqs.ListQueuesInput{QueueNamePrefix: aws.String(prefix), MaxResults: aws.Int32(2), NextToken: first.NextToken}) // Pagination example: reuse the service-issued cursor.
	if err != nil || !reflect.DeepEqual(next.QueueUrls, []string{*queues[2]}) || next.NextToken != nil {
		t.Fatalf("next page: %+v %v", next, err)
	}
	_, err = client.ListQueues(ctx, &sqs.ListQueuesInput{QueueNamePrefix: aws.String("other"), MaxResults: aws.Int32(2), NextToken: first.NextToken}) // Negative example: a cursor cannot change prefix.
	requireHTTPError(t, err, "InvalidParameterValue")
	for _, maximum := range []int32{-1, 1001} {
		_, err = client.ListQueues(ctx, &sqs.ListQueuesInput{MaxResults: aws.Int32(maximum)})
		requireHTTPError(t, err, "InvalidParameterValue")
	}
	found, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(prefix + "a"), QueueOwnerAWSAccountId: aws.String("000000000000")})
	if err != nil || aws.ToString(found.QueueUrl) != *queues[0] {
		t.Fatalf("get URL: %+v %v", found, err)
	}
	_, err = client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(prefix + "a"), QueueOwnerAWSAccountId: aws.String("111111111111")})
	requireHTTPError(t, err, "AWS.SimpleQueueService.NonExistentQueue")
	tags, err := client.ListQueueTags(ctx, &sqs.ListQueueTagsInput{QueueUrl: queues[0]})
	if err != nil || !reflect.DeepEqual(tags.Tags, map[string]string{"環境": "開発", "empty": ""}) {
		t.Fatalf("initial tags: %+v %v", tags, err)
	}
	_, err = client.TagQueue(ctx, &sqs.TagQueueInput{QueueUrl: queues[0], Tags: map[string]string{"環境": "本番", "Team": "lqs"}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.UntagQueue(ctx, &sqs.UntagQueueInput{QueueUrl: queues[0], TagKeys: []string{"empty", "absent"}})
	if err != nil {
		t.Fatal(err)
	}
	tags, err = client.ListQueueTags(ctx, &sqs.ListQueueTagsInput{QueueUrl: queues[0]})
	if err != nil || !reflect.DeepEqual(tags.Tags, map[string]string{"環境": "本番", "Team": "lqs"}) {
		t.Fatalf("tags: %+v %v", tags, err)
	}
	_, err = client.TagQueue(ctx, &sqs.TagQueueInput{QueueUrl: queues[0], Tags: map[string]string{"Team": "should not change", "aws:reserved": "bad"}})
	requireHTTPError(t, err, "InvalidParameterValue")
	after, err := client.ListQueueTags(ctx, &sqs.ListQueueTagsInput{QueueUrl: queues[0]})
	if err != nil || !reflect.DeepEqual(after.Tags, tags.Tags) {
		t.Fatal("invalid tags partially changed queue")
	}
	for _, queue := range queues {
		if _, err := client.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: queue}); err != nil {
			t.Fatal(err)
		}
	}
	empty, err := client.ListQueues(ctx, &sqs.ListQueuesInput{QueueNamePrefix: aws.String(prefix)})
	if err != nil || len(empty.QueueUrls) != 0 {
		t.Fatalf("deleted queues still listed: %+v %v", empty, err)
	}
	_, err = client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(prefix + "a")})
	requireHTTPError(t, err, "AWS.SimpleQueueService.NonExistentQueue")
	_, err = client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(prefix + "a")})
	requireHTTPError(t, err, "AWS.SimpleQueueService.QueueDeletedRecently")
}

func TestManagementMetricsPurgeAndVisibility(t *testing.T) {
	t.Parallel()
	client := newSQSClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	queue := deliveryQueue(t, client, ctx, nil)
	other := deliveryQueue(t, client, ctx, nil)
	send := func(queue *string, body string, delay int32) {
		t.Helper()
		if _, err := client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: queue, MessageBody: aws.String(body), DelaySeconds: delay}); err != nil {
			t.Fatal(err)
		}
	}
	send(queue, "inflight", 0)
	first, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: queue})
	if err != nil || len(first.Messages) != 1 {
		t.Fatalf("receive: %+v %v", first, err)
	}
	send(queue, "visible", 0)
	send(queue, "delayed", 900)
	send(other, "other", 0)
	attrs := pollingAttributes(t, client, ctx, queue)
	if attrs["ApproximateNumberOfMessages"] != "1" || attrs["ApproximateNumberOfMessagesNotVisible"] != "1" || attrs["ApproximateNumberOfMessagesDelayed"] != "1" || attrs["FifoQueue"] != "false" || attrs["CreatedTimestamp"] == "0" || attrs["LastModifiedTimestamp"] == "0" {
		t.Fatalf("metrics: %v", attrs)
	}
	_, err = client.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{QueueUrl: queue, Attributes: map[string]string{"VisibilityTimeout": "0"}})
	if err != nil {
		t.Fatal(err)
	}
	received, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: queue})
	if err != nil || len(received.Messages) != 1 {
		t.Fatalf("zero visibility: %+v %v", received, err)
	}
	attrs = pollingAttributes(t, client, ctx, queue)
	if attrs["VisibilityTimeout"] != "0" || attrs["ApproximateNumberOfMessagesNotVisible"] != "1" {
		t.Fatal(attrs)
	}
	_, err = client.TagQueue(ctx, &sqs.TagQueueInput{QueueUrl: queue, Tags: map[string]string{"keep": "tag"}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.PurgeQueue(ctx, &sqs.PurgeQueueInput{QueueUrl: queue})
	if err != nil {
		t.Fatal(err)
	}
	attrs = pollingAttributes(t, client, ctx, queue)
	for _, key := range []string{"ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible", "ApproximateNumberOfMessagesDelayed"} {
		if attrs[key] != "0" {
			t.Fatal(attrs)
		}
	}
	if attrs["VisibilityTimeout"] != "0" {
		t.Fatal("purge changed settings")
	}
	tags, err := client.ListQueueTags(ctx, &sqs.ListQueueTagsInput{QueueUrl: queue})
	if err != nil || tags.Tags["keep"] != "tag" {
		t.Fatal("purge changed tags", err)
	}
	_, err = client.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: queue, ReceiptHandle: first.Messages[0].ReceiptHandle})
	requireHTTPError(t, err, "ReceiptHandleIsInvalid")
	send(queue, "after purge", 0)
	_, err = client.PurgeQueue(ctx, &sqs.PurgeQueueInput{QueueUrl: queue})
	requireHTTPError(t, err, "AWS.SimpleQueueService.PurgeQueueInProgress")
	if pollingAttributes(t, client, ctx, queue)["ApproximateNumberOfMessages"] != "1" || pollingAttributes(t, client, ctx, other)["ApproximateNumberOfMessages"] != "1" {
		t.Fatal("purge scope/cooldown violated")
	}
	foreign := strings.Replace(*queue, lqsEndpoint(), "http://foreign.invalid", 1)
	_, err = client.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: aws.String(foreign)})
	requireHTTPError(t, err, "InvalidParameterValue")
	if pollingAttributes(t, client, ctx, queue)["ApproximateNumberOfMessages"] != "1" {
		t.Fatal("foreign URL deleted local queue")
	}
}

func managementQueryError(t *testing.T, values url.Values) {
	t.Helper()
	response, err := http.PostForm(lqsEndpoint(), values)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != 400 {
		t.Fatalf("expected Query error, got %d: %s", response.StatusCode, body)
	}
}

func TestManagementQueryProtocol(t *testing.T) {
	t.Parallel()
	name := fmt.Sprintf("query-management-%d", time.Now().UnixNano())
	raw := postQuery(t, lqsEndpoint(), url.Values{"Action": {"CreateQueue"}, "QueueName": {name}, "Attribute.Name": {"VisibilityTimeout"}, "Attribute.Value": {"0"}, "Tag.1.Key": {"環境"}, "Tag.1.Value": {"検証"}})
	var created struct {
		URL string `xml:"CreateQueueResult>QueueUrl"`
	}
	if err := xml.Unmarshal(raw, &created); err != nil || created.URL == "" {
		t.Fatalf("create: %s %v", raw, err)
	}
	queue := created.URL
	raw = postQuery(t, lqsEndpoint(), url.Values{"Action": {"GetQueueUrl"}, "QueueName": {name}})
	var found struct {
		URL string `xml:"GetQueueUrlResult>QueueUrl"`
	}
	if err := xml.Unmarshal(raw, &found); err != nil || found.URL != queue {
		t.Fatalf("get: %s %v", raw, err)
	}
	raw = postQuery(t, lqsEndpoint(), url.Values{"Action": {"ListQueues"}, "QueueNamePrefix": {name}, "MaxResults": {"1"}})
	var listed struct {
		URLs []string `xml:"ListQueuesResult>QueueUrl"`
	}
	if err := xml.Unmarshal(raw, &listed); err != nil || !reflect.DeepEqual(listed.URLs, []string{queue}) {
		t.Fatalf("list: %s %v", raw, err)
	}
	postQuery(t, lqsEndpoint(), url.Values{"Action": {"TagQueue"}, "QueueUrl": {queue}, "Tag.Key": {"team"}, "Tag.Value": {"lqs"}})
	postQuery(t, lqsEndpoint(), url.Values{"Action": {"UntagQueue"}, "QueueUrl": {queue}, "TagKey.1": {"環境"}})
	raw = postQuery(t, lqsEndpoint(), url.Values{"Action": {"ListQueueTags"}, "QueueUrl": {queue}})
	var tags struct {
		Entries []struct {
			Key   string `xml:"Key"`
			Value string `xml:"Value"`
		} `xml:"ListQueueTagsResult>Tag"`
	}
	if err := xml.Unmarshal(raw, &tags); err != nil || len(tags.Entries) != 1 || tags.Entries[0].Key != "team" || tags.Entries[0].Value != "lqs" {
		t.Fatalf("tags: %s %v", raw, err)
	}
	managementQueryError(t, url.Values{"Action": {"SetQueueAttributes"}, "QueueUrl": {queue}, "Attribute.1.Name": {"VisibilityTimeout"}, "Attribute.1.Value": {"10"}, "Attribute.2.Name": {"DelaySeconds"}})
	managementQueryError(t, url.Values{"Action": {"TagQueue"}, "QueueUrl": {queue}, "Tag.1.Key": {"team"}, "Tag.1.Value": {"changed"}, "Tag.2.Value": {"missing key"}})
	raw = postQuery(t, lqsEndpoint(), url.Values{"Action": {"GetQueueAttributes"}, "QueueUrl": {queue}, "AttributeName": {"VisibilityTimeout"}})
	if !strings.Contains(string(raw), "<Value>0</Value>") {
		t.Fatalf("partial update: %s", raw)
	}
	postQuery(t, lqsEndpoint(), url.Values{"Action": {"SendMessage"}, "QueueUrl": {queue}, "MessageBody": {"purge"}})
	parsed, _ := url.Parse(queue)
	postQuery(t, lqsEndpoint()+parsed.Path, url.Values{"Action": {"PurgeQueue"}})
	managementQueryError(t, url.Values{"Action": {"PurgeQueue"}, "QueueUrl": {queue}})
	postQuery(t, lqsEndpoint()+parsed.Path, url.Values{"Action": {"DeleteQueue"}})
	managementQueryError(t, url.Values{"Action": {"ListQueueTags"}, "QueueUrl": {queue}})
	managementQueryError(t, url.Values{"Action": {"CreateQueue"}, "QueueName": {name}})
}
