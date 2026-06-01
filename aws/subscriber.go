package aws

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/klemen-forstneric/ember"
)

type sqsClient interface {
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
}

type SubscriberConfig struct {
	Queues                   map[string]string
	VisibilityTimeoutSeconds int
	NumOfMessages            int32
	WaitTime                 int32
}

type Subscriber struct {
	cfg    SubscriberConfig
	client sqsClient
	logger ember.LoggerCtx

	shutdown chan struct{}
	wg       sync.WaitGroup
}

func NewSubscriber(cfg SubscriberConfig, c sqsClient, l ember.LoggerCtx) *Subscriber {
	return &Subscriber{
		cfg:      cfg,
		client:   c,
		logger:   l,
		shutdown: make(chan struct{}),
	}
}

func (s *Subscriber) Subscribe(ctx context.Context, name string) (<-chan ember.AckableEventEnvelope, error) {
	queue, ok := s.cfg.Queues[name]
	if !ok {
		return nil, fmt.Errorf("no queue for %v", name)
	}

	ch := make(chan ember.AckableEventEnvelope)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		for {
			select {
			default:
			case <-s.shutdown:
				s.logger.Info(ctx, "Shutdown initiated, stopping...")
				return
			}

			in := &sqs.ReceiveMessageInput{
				QueueUrl:            aws.String(queue),
				MaxNumberOfMessages: s.cfg.NumOfMessages,
				WaitTimeSeconds:     s.cfg.WaitTime,
			}

			out, err := s.client.ReceiveMessage(ctx, in)
			if err != nil {
				s.logger.Error(ctx, "Could not receive message", err)
				continue
			}

			if out == nil {
				s.logger.Debug(ctx, "Did not receive a message nor an error")
				continue
			}

			for _, msg := range out.Messages {
				receiptHandle := msg.ReceiptHandle

				if msg.Body == nil {
					s.logger.Info(ctx, "Received message with nil body")
					continue
				}

				var msgEnvelope struct {
					Message string `json:"Message"`
				}

				if err := json.Unmarshal([]byte(*msg.Body), &msgEnvelope); err != nil {
					s.logger.Error(ctx, "Could not unmarshal message envelope", err)
					continue
				}

				var msg message
				if err := json.Unmarshal([]byte(msgEnvelope.Message), &msg); err != nil {
					s.logger.Error(ctx, "Could not unmarshal the message", err)
					continue
				}

				metadata := msg.Metadata
				if metadata == nil {
					metadata = make(ember.Metadata)
				}
				metadata[MetadataKeyCorrelationID] = msg.CorrelationID

				// Create the event envelope.
				envelope := ember.AckableEventEnvelope{
					EventEnvelope: ember.EventEnvelope{
						ID:       msg.ID,
						EntityID: msg.EntityID,
						Event: &ember.MarshaledEvent{
							Type: msg.Type,
							Data: msg.Payload,
						},
						Metadata:  metadata,
						Timestamp: msg.PublishedAt,
					},
					Ack: func() {
						in := &sqs.DeleteMessageInput{
							QueueUrl:      aws.String(queue),
							ReceiptHandle: receiptHandle,
						}

						if _, err := s.client.DeleteMessage(ctx, in); err != nil {
							s.logger.Error(ctx, "Could not delete the message from queue", err)
						}
					},
					Nack: func() {
						in := &sqs.ChangeMessageVisibilityInput{
							QueueUrl:          aws.String(queue),
							ReceiptHandle:     receiptHandle,
							VisibilityTimeout: int32(s.cfg.VisibilityTimeoutSeconds),
						}

						if _, err := s.client.ChangeMessageVisibility(ctx, in); err != nil {
							s.logger.Error(ctx, "Could not change message visibility", err)
						}
					},
				}

				select {
				case ch <- envelope:
				case <-s.shutdown:
					s.logger.Info(ctx, "Shutdown initiated, stopping...")
					return
				}
			}
		}
	}()

	return ch, nil
}

func (s *Subscriber) Stop() {
	close(s.shutdown)
	s.wg.Wait()
}
