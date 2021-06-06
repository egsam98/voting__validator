package amqp

import (
	"github.com/Shopify/sarama"
	votingpb "github.com/egsam98/voting/proto"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"

	"github.com/egsam98/voting/validator/services"
)

// ValidateVoter is sarama consumer's handler to validate voter
type ValidateVoter struct {
	topicDead         string
	service           *services.VoterValidator
	votesProducerDead sarama.AsyncProducer
}

func NewValidateVoter(
	topicDead string,
	service *services.VoterValidator,
	votesProducerDead sarama.AsyncProducer,
) *ValidateVoter {
	return &ValidateVoter{
		topicDead:         topicDead,
		service:           service,
		votesProducerDead: votesProducerDead,
	}
}

func (v *ValidateVoter) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
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
			v.votesProducerDead.Input() <- &sarama.ProducerMessage{
				Topic: v.topicDead,
				Value: sarama.ByteEncoder(msg.Value),
			}
			log.Error().Stack().Err(err).Msg("handlers.amqp: Vote handling error")
		}

		session.MarkMessage(msg, "")
	}
	return nil
}

func (v *ValidateVoter) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (v *ValidateVoter) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}
