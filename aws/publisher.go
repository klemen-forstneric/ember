package aws

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/klemen-forstneric/ember"
	"github.com/pkg/errors"
)

type snsClient interface {
	Publish(ctx context.Context, params *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error)
}

type PublisherConfig struct {
	Topic string
}

type Publisher struct {
	cfg    PublisherConfig
	client snsClient
	logger ember.LoggerCtx
}

func NewPublisher(cfg PublisherConfig, c snsClient, l ember.LoggerCtx) *Publisher {
	return &Publisher{
		cfg:    cfg,
		client: c,
		logger: l,
	}
}

func (p *Publisher) Publish(ctx context.Context, envelopes []ember.EventEnvelope) error {
	for _, e := range envelopes {
		correlationId, ok := e.Metadata[MetadataKeyCorrelationID].(string)
		if !ok {
			return fmt.Errorf("invalid metadata, missing key '%v'", MetadataKeyCorrelationID)
		}

		m := &message{
			ID:            e.ID,
			CorrelationID: correlationId,
			EntityID:      e.EntityID,
			Type:          e.Event.Type,
			Metadata:      e.Metadata,
			Payload:       e.Event.Data,
			PublishedAt:   e.Timestamp,
		}

		payload, err := json.Marshal(m)
		if err != nil {
			return errors.Wrap(err, "could not marshal the message")
		}

		in := &sns.PublishInput{
			MessageDeduplicationId: aws.String(e.ID),
			MessageGroupId:         aws.String(e.EntityID),
			Message:                aws.String(string(payload)),
			TopicArn:               aws.String(p.cfg.Topic),
		}

		if _, err := p.client.Publish(ctx, in); err != nil {
			return errors.Wrap(err, "could not publish the message")
		}
	}

	return nil
}
