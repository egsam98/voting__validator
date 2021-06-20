package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/rs/zerolog/pkgerrors"

	"github.com/egsam98/voting/validator/handlers/amqp"
	"github.com/egsam98/voting/validator/services/votervalidator"
)

var envs struct {
	Kafka struct {
		Addr  string `envconfig:"KAFKA_ADDR"`
		Topic struct {
			IsDead    bool   `envconfig:"KAFKA_TOPIC_IS_DEAD"`
			Name      string `envconfig:"KAFKA_TOPIC_NAME"`
			ChainName string `envconfig:"KAFKA_TOPIC_CHAIN_NAME"`
		}
		Consumer struct {
			GroupID             string        `envconfig:"KAFKA_CONSUMER_GROUP_ID" required:"true"`
			ConsumptionInterval time.Duration `envconfig:"KAFKA_CONSUMER_CONSUMPTION_INTERVAL" default:"10s"`
		}
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

	validatorService := votervalidator.New(envs.Gosuslugi.Host)

	closeConsumer, err := startConsumer(ctx, validatorService)
	if err != nil {
		return err
	}

	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, syscall.SIGINT)

	sig := <-sigint
	log.Info().Msgf("main: Waiting consumer group %q to complete", envs.Kafka.Consumer.GroupID)
	cancel()
	if err := closeConsumer(); err != nil {
		return err
	}
	log.Info().Msgf("main: Terminated via signal %q", sig)
	return nil
}

// startConsumer selects consumer with type based on "KAFKA_TOPIC_IS_DEAD" env value and starts listening
func startConsumer(ctx context.Context, validatorService *votervalidator.VoterValidator) (func() error, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Errors = true
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	kafkaClient, err := sarama.NewClient([]string{envs.Kafka.Addr}, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to Kafka broker")
	}

	producer, err := sarama.NewSyncProducerFromClient(kafkaClient)
	if err != nil {
		return nil, errors.Wrap(err, "failed to init Kafka producer")
	}

	var options []amqp.ValidateVoterHandlerOption
	if envs.Kafka.Topic.IsDead {
		options = append(options, amqp.WithTopicDead(envs.Kafka.Consumer.ConsumptionInterval))
	}

	validateVoterHandler := amqp.NewValidateVoterHandler(
		envs.Kafka.Topic.ChainName,
		validatorService,
		producer,
		options...,
	)

	consumerGroup, err := sarama.NewConsumerGroupFromClient(envs.Kafka.Consumer.GroupID, kafkaClient)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to init Kafka consumer group %q", envs.Kafka.Consumer.GroupID)
	}

	go func() {
		for err := range consumerGroup.Errors() {
			log.Error().Stack().Err(err).Msg("main: Consumer error")
		}
	}()

	go func() {
		format := "main: Consuming from topic=%s, group ID=%s"
		if envs.Kafka.Topic.IsDead {
			format = "main: Consuming from dead topic=%s, group ID=%s"
		}
		log.Info().Msgf(format, envs.Kafka.Topic.Name, envs.Kafka.Consumer.GroupID)

		for {
			if err := consumerGroup.Consume(
				ctx,
				[]string{envs.Kafka.Topic.Name},
				validateVoterHandler,
			); err != nil {
				log.Fatal().Err(err).Msgf("main: Failed to consume from topic=%s", envs.Kafka.Topic.Name)
			}

			if ctx.Err() != nil {
				return
			}
		}
	}()

	return func() error {
		if err := producer.Close(); err != nil {
			return errors.Wrap(err, "failed to close Kafka producer")
		}
		if err := consumerGroup.Close(); err != nil {
			return errors.Wrapf(err, "failed to close consumer group %q", envs.Kafka.Consumer.GroupID)
		}
		return nil
	}, nil
}
