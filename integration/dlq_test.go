package integration_test

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

func TestAWSSDKV2DeadLetterQueues(t *testing.T) {
	for _, fifo := range []bool{false, true} {
		t.Run(fmt.Sprintf("FIFO=%v", fifo), func(t *testing.T) {
			client := newSQSClient(t)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			create := func(name string, extra map[string]string) *string {
				t.Helper()
				attrs := map[string]string{}
				if fifo {
					name += ".fifo"
					attrs["FifoQueue"] = "true"
					attrs["ContentBasedDeduplication"] = "true"
				}
				for k, v := range extra {
					attrs[k] = v
				}
				out, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name), Attributes: attrs})
				if err != nil {
					t.Fatal(err)
				}
				return out.QueueUrl
			}
			prefix := fmt.Sprintf("dlq-%d", time.Now().UnixNano())
			deadURL := create(prefix+"-dead", nil)
			attrs, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: deadURL, AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn}})
			if err != nil {
				t.Fatal(err)
			}
			arn := attrs.Attributes["QueueArn"]
			if arn == "" {
				t.Fatal("missing DLQ ARN")
			}
			policy, _ := json.Marshal(map[string]any{"deadLetterTargetArn": arn, "maxReceiveCount": 2})
			sourceURL := create(prefix+"-source", map[string]string{"RedrivePolicy": string(policy)})
			again := create(prefix+"-source", map[string]string{"RedrivePolicy": string(policy)})
			if aws.ToString(again) != aws.ToString(sourceURL) {
				t.Fatal("CreateQueue is not idempotent")
			}
			attrs, err = client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: sourceURL, AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameRedrivePolicy}})
			if err != nil {
				t.Fatal(err)
			}
			var gotPolicy struct {
				ARN   string `json:"deadLetterTargetArn"`
				Count int    `json:"maxReceiveCount"`
			}
			if err := json.Unmarshal([]byte(attrs.Attributes["RedrivePolicy"]), &gotPolicy); err != nil {
				t.Fatal(err)
			}
			if gotPolicy.ARN != arn || gotPolicy.Count != 2 {
				t.Fatalf("policy = %+v", gotPolicy)
			}

			secondURL := create(prefix+"-second", nil)
			_, err = client.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{QueueUrl: secondURL, Attributes: map[string]string{"RedrivePolicy": string(policy)}})
			if err != nil {
				t.Fatal(err)
			}
			var sources []string
			for _, max := range []int32{-1, 0, 1001} {
				_, err := client.ListDeadLetterSourceQueues(ctx, &sqs.ListDeadLetterSourceQueuesInput{QueueUrl: deadURL, MaxResults: aws.Int32(max)})
				requireHTTPError(t, err, "InvalidParameterValue")
			}
			_, err = client.ListDeadLetterSourceQueues(ctx, &sqs.ListDeadLetterSourceQueuesInput{QueueUrl: deadURL, NextToken: aws.String("invalid")}) // Example of an invalid pagination cursor.
			requireHTTPError(t, err, "InvalidParameterValue")
			var cursor *string
			for {
				listed, err := client.ListDeadLetterSourceQueues(ctx, &sqs.ListDeadLetterSourceQueuesInput{QueueUrl: deadURL, MaxResults: aws.Int32(1), NextToken: cursor}) // Example of SDK pagination using the prior response.
				if err != nil {
					t.Fatal(err)
				}
				sources = append(sources, listed.QueueUrls...)
				cursor = listed.NextToken
				if cursor == nil {
					break
				}
				if len(sources) > 2 {
					t.Fatal("pagination does not terminate")
				}
			}
			if len(sources) != 2 || sources[0] != *secondURL || sources[1] != *sourceURL {
				t.Fatalf("sources = %v", sources)
			}
			_, err = client.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{QueueUrl: secondURL, Attributes: map[string]string{"RedrivePolicy": ""}})
			if err != nil {
				t.Fatal(err)
			}
			listed, err := client.ListDeadLetterSourceQueues(ctx, &sqs.ListDeadLetterSourceQueuesInput{QueueUrl: deadURL})
			if err != nil || len(listed.QueueUrls) != 1 || listed.QueueUrls[0] != *sourceURL {
				t.Fatalf("sources after removal: %+v %v", listed, err)
			}

			send := &sqs.SendMessageInput{QueueUrl: sourceURL, MessageBody: aws.String("failed message")}
			if fifo {
				send.MessageGroupId = aws.String("group-a")
			}
			sent, err := client.SendMessage(ctx, send)
			if err != nil {
				t.Fatal(err)
			}
			receive := func(queueURL *string) []types.Message {
				t.Helper()
				out, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: queueURL, MaxNumberOfMessages: 10})
				if err != nil {
					t.Fatal(err)
				}
				return out.Messages
			}
			for attempt := 1; attempt <= 2; attempt++ {
				messages := receive(sourceURL)
				if len(messages) != 1 || aws.ToString(messages[0].MessageId) != aws.ToString(sent.MessageId) {
					t.Fatalf("attempt %d: %+v", attempt, messages)
				}
				if len(receive(sourceURL)) != 0 || len(receive(deadURL)) != 0 {
					t.Fatal("in-flight message was delivered or moved too early")
				}
				_, err := client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{QueueUrl: sourceURL, ReceiptHandle: messages[0].ReceiptHandle, VisibilityTimeout: 0})
				if err != nil {
					t.Fatal(err)
				}
			}
			if len(receive(sourceURL)) != 0 {
				t.Fatal("message delivered beyond maxReceiveCount")
			}
			dead := receive(deadURL)
			if len(dead) != 1 || aws.ToString(dead[0].MessageId) != aws.ToString(sent.MessageId) || aws.ToString(dead[0].Body) != "failed message" {
				t.Fatalf("DLQ messages: %+v", dead)
			}
			deleteMessage(t, ctx, client, deadURL, dead[0].ReceiptHandle)
			if len(receive(deadURL)) != 0 {
				t.Fatal("duplicate DLQ delivery after deletion")
			}

			invalidPolicies := []string{
				`not-json`, `{}`, fmt.Sprintf(`{"deadLetterTargetArn":%q,"maxReceiveCount":0}`, arn),
				fmt.Sprintf(`{"deadLetterTargetArn":%q,"maxReceiveCount":1001}`, arn),
				fmt.Sprintf(`{"deadLetterTargetArn":%q,"maxReceiveCount":1.5}`, arn),
				`{"deadLetterTargetArn":"arn:aws:sqs:other:000000000000:dead","maxReceiveCount":1}`,
				fmt.Sprintf(`{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:%s-missing","maxReceiveCount":1}`, prefix),
			}
			oppositeName := prefix + "-opposite"
			oppositeAttributes := map[string]string{}
			if !fifo {
				oppositeName += ".fifo"
				oppositeAttributes["FifoQueue"] = "true"
			}
			_, err = client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(oppositeName), Attributes: oppositeAttributes})
			if err != nil {
				t.Fatal(err)
			}
			invalidPolicies = append(invalidPolicies, fmt.Sprintf(`{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:%s","maxReceiveCount":1}`, oppositeName))
			for _, invalid := range invalidPolicies {
				_, err := client.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{QueueUrl: sourceURL, Attributes: map[string]string{"RedrivePolicy": invalid}})
				requireHTTPError(t, err, "InvalidParameterValue")
			}
			// Failed updates must leave the original policy in place.
			after, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: sourceURL, AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameRedrivePolicy}})
			if err != nil || after.Attributes["RedrivePolicy"] != attrs.Attributes["RedrivePolicy"] {
				t.Fatalf("policy changed after invalid update: %+v %v", after, err)
			}
		})
	}
}

func requireHTTPError(t *testing.T, err error, code string) {
	t.Helper()
	var responseError *smithyhttp.ResponseError
	if apiErrorCode(err) != code || !errors.As(err, &responseError) || responseError.HTTPStatusCode() != http.StatusBadRequest {
		t.Fatalf("want HTTP 400/%s, got %v", code, err)
	}
}

func TestQueryDeadLetterPolicy(t *testing.T) {
	endpoint := lqsEndpoint()
	name := fmt.Sprintf("query-dlq-%d", time.Now().UnixNano())
	deadURL := endpoint + "/000000000000/" + name + "-dead"
	sourceURL := endpoint + "/000000000000/" + name + "-source"
	for _, suffix := range []string{"-dead", "-source"} {
		postQuery(t, endpoint, url.Values{"Action": {"CreateQueue"}, "QueueName": {name + suffix}})
	}
	policy := fmt.Sprintf(`{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:%s-dead","maxReceiveCount":"1"}`, name)
	postQuery(t, endpoint, url.Values{"Action": {"SetQueueAttributes"}, "QueueUrl": {sourceURL}, "Attribute.1.Name": {"RedrivePolicy"}, "Attribute.1.Value": {policy}})
	body := postQuery(t, endpoint, url.Values{"Action": {"GetQueueAttributes"}, "QueueUrl": {sourceURL}, "AttributeName.1": {"RedrivePolicy"}})
	var attributes struct {
		Values []struct {
			Name  string
			Value string
		} `xml:"GetQueueAttributesResult>Attribute"`
	}
	if err := xml.Unmarshal(body, &attributes); err != nil {
		t.Fatal(err)
	}
	if len(attributes.Values) != 1 || attributes.Values[0].Name != "RedrivePolicy" {
		t.Fatalf("attributes = %s", body)
	}
	body = postQuery(t, endpoint, url.Values{"Action": {"ListDeadLetterSourceQueues"}, "QueueUrl": {deadURL}})
	var listed struct {
		URLs []string `xml:"ListDeadLetterSourceQueuesResult>QueueUrl"`
	}
	if err := xml.Unmarshal(body, &listed); err != nil {
		t.Fatal(err)
	}
	if len(listed.URLs) != 1 || listed.URLs[0] != sourceURL {
		t.Fatalf("sources = %s", body)
	}
}
