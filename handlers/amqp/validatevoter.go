package amqp

import (
	"time"

	"github.com/Shopify/sarama"
	votingpb "github.com/egsam98/voting/proto"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"

	"github.com/egsam98/voting/validator/services/votervalidator"
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
	validator           *votervalidator.VoterValidator
	producer            sarama.SyncProducer
}

func NewValidateVoterHandler(
	chainTopic string,
	validator *votervalidator.VoterValidator,
	producer sarama.SyncProducer,
	options ...ValidateVoterHandlerOption,
) *ValidateVoterHandler {
	h := &ValidateVoterHandler{
		chainTopic: chainTopic,
		validator:  validator,
		producer:   producer,
	}

	for _, option := range options {
		option(h)
	}

	return h
}

// TODO: handle unexpedcted errors
func (v *ValidateVoterHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		vote := &votingpb.Vote{}
		if err := proto.Unmarshal(msg.Value, vote); err != nil {
			return errors.Wrapf(err, "failed to unmarshal data=%s", string(msg.Value))
		}

		if vote.CandidateId == 0 {
			return errors.Errorf("candidate ID must be non-empty")
		}
		if vote.Voter.GetPassport() == "" {
			return errors.Errorf("voter's passport must be non-empty")
		}
		if vote.Voter.GetFullname() == "" {
			return errors.Errorf("voter's fullname must be non-empty")
		}

		log.Debug().
			Str("topic", msg.Topic).
			Int32("partition", msg.Partition).
			Int64("offset", msg.Offset).
			Interface("vote", vote).
			Msg("amqp.ValidateVoterHandler: Received message")

		upVoter, err := v.validator.Run(session.Context(), vote.Voter)
		if err != nil {
			if errors.Is(err, votervalidator.ErrInvalidVoter) {
				log.Warn().Err(err).Msg("amqp.ValidateVoterHandler: Invalid voter")
				continue
			}

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

			log.Error().Stack().Err(err).Msg("amqp.ValidateVoterHandler: Vote handling error")
			return nil
		}

		vote.Voter = upVoter
		updB, err := proto.Marshal(vote)
		if err != nil {
			return errors.Wrapf(err, "failed to marshal %T to bytes", vote)
		}

		if _, _, err := v.producer.SendMessage(&sarama.ProducerMessage{
			Topic: v.chainTopic,
			Value: sarama.ByteEncoder(updB),
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
