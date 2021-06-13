package amqp

import (
	"time"

	"github.com/Shopify/sarama"
	votingpb "github.com/egsam98/voting/proto"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"

	"github.com/egsam98/voting/validator/services"
)

type ValidateVoterHandlerOption func(*ValidateVoterHandler)

// WithTopicDead treats topic using dead letter queue pattern with provided consumption interval
func WithTopicDead(consumptionInterval time.Duration) ValidateVoterHandlerOption {
	return func(handler *ValidateVoterHandler) {
		handler.isTopicDead = true
		handler.consumptionInterval = consumptionInterval
	}
}

// ValidateVoterHandler is sarama consumer's handler to validate voter
type ValidateVoterHandler struct {
	isTopicDead         bool
	consumptionInterval time.Duration
	chainTopic          string
	service             *services.VoterValidator
	producer            sarama.SyncProducer
}

func NewValidateVoterHandler(
	chainTopic string,
	service *services.VoterValidator,
	producer sarama.SyncProducer,
	options ...ValidateVoterHandlerOption,
) *ValidateVoterHandler {
	h := &ValidateVoterHandler{
		chainTopic: chainTopic,
		service:    service,
		producer:   producer,
	}

	for _, option := range options {
		option(h)
	}

	return h
}

func (v *ValidateVoterHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		vote := &votingpb.Vote{}
		if err := proto.Unmarshal(msg.Value, vote); err != nil {
			return errors.Wrapf(err, "failed to unmarshal data=%s", string(msg.Value))
		}

		log.Debug().
			Str("topic", msg.Topic).
			Int32("partition", msg.Partition).
			Int64("offset", msg.Offset).
			Interface("vote", vote).
			Msg("handlers.amqp: Received message")

		if err := v.service.Run(session.Context(), vote); err != nil {
			if v.isTopicDead {
				time.Sleep(v.consumptionInterval)
				return err
			}

			topicDead := msg.Topic + ".dead"
			if _, _, err := v.producer.SendMessage(&sarama.ProducerMessage{
				Topic: topicDead,
				Value: sarama.ByteEncoder(msg.Value),
			}); err != nil {
				return errors.Wrapf(err, "failed to send message to topic %q", topicDead)
			}

			log.Error().Stack().Err(err).Msg("handlers.amqp: Vote handling error")
			return nil
		}

		if _, _, err := v.producer.SendMessage(&sarama.ProducerMessage{
			Topic: v.chainTopic,
			Value: sarama.ByteEncoder(msg.Value),
		}); err != nil {
			return errors.Wrapf(err, "failed to send message to topic %q", v.chainTopic)
		}

		if v.isTopicDead {
			time.Sleep(v.consumptionInterval)
		}

		session.MarkMessage(msg, "")
	}
	return nil
}

func (v *ValidateVoterHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (v *ValidateVoterHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}
