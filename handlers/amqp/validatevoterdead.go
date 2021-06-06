package amqp

import (
	"time"

	"github.com/Shopify/sarama"
	votingpb "github.com/egsam98/voting/proto"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"

	"github.com/egsam98/voting/validator/services"
)

// ValidateVoterDead is sarama consumer's handler to validate voter from dead topic
type ValidateVoterDead struct {
	interval time.Duration
	service  *services.VoterValidator
}

func NewValidateVoterDead(
	interval time.Duration,
	service *services.VoterValidator,
) *ValidateVoterDead {
	return &ValidateVoterDead{
		interval: interval,
		service:  service,
	}
}

func (v *ValidateVoterDead) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
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
			time.Sleep(v.interval)
			return err
		}

		time.Sleep(v.interval)
		session.MarkMessage(msg, "")
	}
	return nil
}

func (v *ValidateVoterDead) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (v *ValidateVoterDead) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}
