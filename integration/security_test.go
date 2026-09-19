package integration_test

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// Test-only, self-asserted identities. The server must explicitly enable
// LQS_TRUST_PRINCIPAL_HEADER; these are not authenticated AWS identities.
type principalTransport struct{ principal string }

func (transport principalTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	request = request.Clone(request.Context())
	request.Header.Set("x-lqs-principal", transport.principal)
	return http.DefaultTransport.RoundTrip(request)
}
func securityClient(t *testing.T, principal string) *sqs.Client {
	options := newSQSClient(t).Options()
	options.HTTPClient = &http.Client{Transport: principalTransport{principal: principal}}
	return sqs.New(options)
}
func accessPolicy(deny bool) string {
	statements := []any{
		map[string]any{"Sid": "Admin", "Effect": "Allow", "Principal": map[string]string{"AWS": "111111111111"}, "Action": "sqs:*", "Resource": "*"},
		map[string]any{"Sid": "Producer", "Effect": "Allow", "Principal": map[string]string{"AWS": "222222222222"}, "Action": "sqs:SendMessage", "Resource": "*"},
	}
	if deny {
		statements = append(statements, map[string]any{"Sid": "DenySend", "Effect": "Deny", "Principal": "*", "Action": "sqs:SendMessage", "Resource": "*"})
	}
	encoded, _ := json.Marshal(map[string]any{"Version": "2012-10-17", "Statement": statements})
	return string(encoded)
}
func requireAccessDenied(t *testing.T, err error) {
	t.Helper()
	var response *smithyhttp.ResponseError
	if apiErrorCode(err) != "AccessDenied" || !errors.As(err, &response) || response.HTTPStatusCode() != http.StatusForbidden {
		t.Fatalf("expected AccessDenied/403, got %v", err)
	}
}

func TestSecurityPolicyPermissionsAndBatchAuthorization(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin, producer, outsider := securityClient(t, "111111111111"), securityClient(t, "222222222222"), securityClient(t, "333333333333")
	queue := deliveryQueue(t, admin, ctx, map[string]string{"Policy": accessPolicy(false)})
	_, err := outsider.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: queue, MessageBody: aws.String("denied")})
	requireAccessDenied(t, err)
	_, err = outsider.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: queue, Entries: []types.SendMessageBatchRequestEntry{{Id: aws.String("one"), MessageBody: aws.String("denied")}}})
	requireAccessDenied(t, err)
	_, err = producer.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: queue, Entries: []types.SendMessageBatchRequestEntry{{Id: aws.String("one"), MessageBody: aws.String("allowed")}}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = outsider.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: queue})
	requireAccessDenied(t, err)
	_, err = producer.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{QueueUrl: queue, Attributes: map[string]string{"Policy": ""}})
	requireAccessDenied(t, err)
	_, err = admin.AddPermission(ctx, &sqs.AddPermissionInput{QueueUrl: queue, Label: aws.String("ReadGrant"), AWSAccountIds: []string{"333333333333"}, Actions: []string{"ReceiveMessage"}})
	if err != nil {
		t.Fatal(err)
	}
	received, err := outsider.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: queue})
	if err != nil || len(received.Messages) != 1 || aws.ToString(received.Messages[0].Body) != "allowed" {
		t.Fatalf("granted receive: %+v %v", received, err)
	}
	_, err = outsider.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: queue, ReceiptHandle: received.Messages[0].ReceiptHandle})
	requireAccessDenied(t, err)
	_, err = outsider.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{QueueUrl: queue, Entries: []types.DeleteMessageBatchRequestEntry{{Id: aws.String("one"), ReceiptHandle: received.Messages[0].ReceiptHandle}}})
	requireAccessDenied(t, err)
	_, err = outsider.AddPermission(ctx, &sqs.AddPermissionInput{QueueUrl: queue, Label: aws.String("Escalation"), AWSAccountIds: []string{"333333333333"}, Actions: []string{"*"}})
	requireAccessDenied(t, err)
	_, err = admin.RemovePermission(ctx, &sqs.RemovePermissionInput{QueueUrl: queue, Label: aws.String("ReadGrant")})
	if err != nil {
		t.Fatal(err)
	}
	_, err = outsider.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: queue})
	requireAccessDenied(t, err)
	deleteMessage(t, ctx, admin, queue, received.Messages[0].ReceiptHandle)
	_, err = admin.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{QueueUrl: queue, Attributes: map[string]string{"Policy": accessPolicy(true)}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = admin.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: queue, MessageBody: aws.String("admin explicitly denied")})
	requireAccessDenied(t, err)
	attrs := pollingAttributes(t, admin, ctx, queue)
	if attrs["Policy"] != accessPolicy(true) || attrs["ApproximateNumberOfMessages"] != "0" {
		t.Fatal(attrs)
	}
}

func TestSecurityEncryptionConfigurationAndFailClosedPolicies(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin := securityClient(t, "111111111111")
	queue := deliveryQueue(t, admin, ctx, map[string]string{"Policy": accessPolicy(false), "SqsManagedSseEnabled": "true"})
	attrs := pollingAttributes(t, admin, ctx, queue)
	if attrs["SqsManagedSseEnabled"] != "true" || attrs["KmsMasterKeyId"] != "" || attrs["KmsDataKeyReusePeriodSeconds"] != "300" {
		t.Fatal(attrs)
	}
	for _, invalid := range []map[string]string{
		{"SqsManagedSseEnabled": "true", "KmsMasterKeyId": "alias/example"},
		{"KmsDataKeyReusePeriodSeconds": "59"}, {"KmsDataKeyReusePeriodSeconds": "86401"},
		{"SqsManagedSseEnabled": "yes"}, {"KmsMasterKeyId": "invalid key"},
		{"Policy": `{"Statement":[{"Effect":"Allow","Principal":"*","Action":"sqs:*","Resource":"*","Condition":{"Bool":{"aws:SecureTransport":"true"}}}]}`},
	} {
		_, err := admin.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{QueueUrl: queue, Attributes: invalid})
		requireHTTPError(t, err, "InvalidParameterValue")
		current := pollingAttributes(t, admin, ctx, queue)
		if current["Policy"] != attrs["Policy"] || current["SqsManagedSseEnabled"] != "true" {
			t.Fatal("partial security update", current)
		}
	}
	_, err := admin.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: queue, MessageBody: aws.String("configuration-only")})
	if err != nil {
		t.Fatal(err)
	}
	message, err := admin.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: queue, MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll}})
	if err != nil || len(message.Messages) != 1 || message.Messages[0].Attributes["SqsManagedSseEnabled"] != "false" {
		t.Fatalf("must not claim payload encryption: %+v %v", message, err)
	}
	for _, config := range []map[string]string{
		{"KmsMasterKeyId": "alias/example", "KmsDataKeyReusePeriodSeconds": "60"},
		{"KmsMasterKeyId": "arn:aws:kms:us-east-1:000000000000:key/example"},
		{"KmsMasterKeyId": ""},
		{"SqsManagedSseEnabled": "true"}, {"SqsManagedSseEnabled": "false"},
	} {
		if _, err := admin.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{QueueUrl: queue, Attributes: config}); err != nil {
			t.Fatal(err)
		}
		current := pollingAttributes(t, admin, ctx, queue)
		for key, value := range config {
			if current[key] != value {
				t.Fatalf("%s: %v", key, current)
			}
		}
	}
	_, err = admin.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{QueueUrl: queue, Attributes: map[string]string{"Policy": ""}})
	if err != nil {
		t.Fatal(err)
	}
	// Removing a policy deliberately returns to local-open compatibility mode.
	_, err = newSQSClient(t).SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: queue, MessageBody: aws.String("open again")})
	if err != nil {
		t.Fatal(err)
	}
}

func securityQuery(t *testing.T, principal string, values url.Values, status int) []byte {
	t.Helper()
	request, err := http.NewRequest("POST", lqsEndpoint(), strings.NewReader(values.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("x-lqs-principal", principal)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != status {
		t.Fatalf("Query status %d: %s", response.StatusCode, body)
	}
	return body
}

func TestSecurityQueryPermissionAndSSEAttributes(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin := securityClient(t, "111111111111")
	queue := deliveryQueue(t, admin, ctx, map[string]string{"Policy": accessPolicy(false)})
	securityQuery(t, "111111111111", url.Values{"Action": {"AddPermission"}, "QueueUrl": {*queue}, "Label": {"QueryGrant"}, "AWSAccountId.1": {"444444444444"}, "ActionName.1": {"SendMessage"}}, 200)
	securityQuery(t, "444444444444", url.Values{"Action": {"SendMessage"}, "QueueUrl": {*queue}, "MessageBody": {"granted"}}, 200)
	denied := securityQuery(t, "444444444444", url.Values{"Action": {"ReceiveMessage"}, "QueueUrl": {*queue}}, 403)
	if !strings.Contains(string(denied), "<Code>AccessDenied</Code>") {
		t.Fatalf("error code: %s", denied)
	}
	securityQuery(t, "111111111111", url.Values{"Action": {"RemovePermission"}, "QueueUrl": {*queue}, "Label": {"QueryGrant"}}, 200)
	securityQuery(t, "444444444444", url.Values{"Action": {"SendMessage"}, "QueueUrl": {*queue}, "MessageBody": {"revoked"}}, 403)
	securityQuery(t, "111111111111", url.Values{"Action": {"SetQueueAttributes"}, "QueueUrl": {*queue}, "Attribute.1.Name": {"KmsMasterKeyId"}, "Attribute.1.Value": {"alias/query"}, "Attribute.2.Name": {"KmsDataKeyReusePeriodSeconds"}, "Attribute.2.Value": {"60"}}, 200)
	body := securityQuery(t, "111111111111", url.Values{"Action": {"GetQueueAttributes"}, "QueueUrl": {*queue}, "AttributeName": {"All"}}, 200)
	var result struct {
		Attributes []struct {
			Name  string `xml:"Name"`
			Value string `xml:"Value"`
		} `xml:"GetQueueAttributesResult>Attribute"`
	}
	if err := xml.Unmarshal(body, &result); err != nil {
		t.Fatal(err)
	}
	attrs := map[string]string{}
	for _, item := range result.Attributes {
		attrs[item.Name] = item.Value
	}
	if attrs["KmsMasterKeyId"] != "alias/query" || attrs["KmsDataKeyReusePeriodSeconds"] != "60" || !strings.Contains(attrs["Policy"], "Admin") || strings.Contains(attrs["Policy"], "QueryGrant") {
		t.Fatal(attrs)
	}
}
