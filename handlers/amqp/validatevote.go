package amqp

import (
	"github.com/Shopify/sarama"
	votingpb "github.com/egsam98/voting/proto"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"

	"github.com/egsam98/voting/validator/services"
)

// ValidateVote is sarama consumer's handler to validate vote
type ValidateVote struct {
	service *services.VoterValidator
}

func NewValidateVote(service *services.VoterValidator) *ValidateVote {
	return &ValidateVote{service: service}
}

func (v *ValidateVote) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
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
			return err
		}

		session.MarkMessage(msg, "")
	}
	return nil
}

func (v *ValidateVote) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (v *ValidateVote) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}
