package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/rs/zerolog/pkgerrors"

	"github.com/egsam98/voting/validator/handlers/amqp"
	"github.com/egsam98/voting/validator/services"
)

var envs struct {
	Kafka struct {
		Addr  string `envconfig:"KAFKA_ADDR"`
		Topic string `envconfig:"KAFKA_TOPIC"`
	}
	Gosuslugi struct {
		Host string `envconfig:"GOSUSLUGI_HOST" required:"true"`
	}
}

func main() {
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	if err := run(); err != nil {
		log.Fatal().Stack().Err(err).Msg("main: Fatal error")
	}
}

func run() error {
	if err := godotenv.Load(); err != nil && !os.IsNotExist(err) {
		log.Warn().Err(err).Msg("main: Read ENVs from .env file")
	}
	if err := envconfig.Process("", &envs); err != nil {
		return errors.Wrap(err, "failed to parse ENVs to struct")
	}

	log.Info().
		Interface("envs", envs).
		Msg("main: ENVs")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := sarama.NewConfig()
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumerGroup, err := sarama.NewConsumerGroup([]string{envs.Kafka.Addr}, envs.Kafka.Topic, cfg)
	if err != nil {
		return errors.Wrapf(err, "failed to init consumer group %q", envs.Kafka.Topic)
	}

	validateVoteHandler := amqp.NewValidateVote(
		services.NewVoterValidator(envs.Gosuslugi.Host),
	)

	go func() {
		for err := range consumerGroup.Errors() {
			log.Error().Stack().Err(err).Msg("handlers.amqp: Consumer error")
		}
	}()
	go func() {
		for {
			if err := consumerGroup.Consume(ctx, []string{envs.Kafka.Topic}, validateVoteHandler); err != nil {
				log.Fatal().Err(err).Msgf("main: Failed to consume from topic %q", envs.Kafka.Topic)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, syscall.SIGINT)

	sig := <-sigint
	log.Info().Msgf("main: Waiting consumer group %q to complete", envs.Kafka.Topic)
	cancel()
	if err := consumerGroup.Close(); err != nil {
		return errors.Wrapf(err, "failed to close consumer group %q", envs.Kafka.Topic)
	}
	log.Info().Msgf("main: Terminated via signal %q", sig)
	return nil
}
