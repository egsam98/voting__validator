package main

import (
	"context"
	"net/http"
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
	"github.com/egsam98/voting/validator/handlers/rest"
	"github.com/egsam98/voting/validator/services/votervalidator"
)

var envs struct {
	Web struct {
		Addr            string        `envconfig:"WEB_ADDR" default:":3000"`
		ShutdownTimeout time.Duration `envconfig:"WEB_SHUTDOWN_TIMEOUT" default:"5s"`
	}
	Kafka struct {
		Addr  string `envconfig:"KAFKA_ADDR" required:"true"`
		Topic struct {
			IsDead    bool   `envconfig:"KAFKA_TOPIC_IS_DEAD" default:"false"`
			Name      string `envconfig:"KAFKA_TOPIC_NAME" required:"true"`
			ChainName string `envconfig:"KAFKA_TOPIC_CHAIN_NAME" required:"true"`
		}
		Consumer struct {
			GroupID             string        `envconfig:"KAFKA_CONSUMER_GROUP_ID" default:"validator"`
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

	validatorService := votervalidator.New(envs.Gosuslugi.Host)

	cfg := sarama.NewConfig()
	cfg.Producer.Return.Errors = true
	cfg.Producer.Return.Successes = true
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	kafkaClient, err := sarama.NewClient([]string{envs.Kafka.Addr}, cfg)
	if err != nil {
		return errors.Wrap(err, "failed to connect to Kafka broker")
	}

	saramaAdmin, err := sarama.NewClusterAdminFromClient(kafkaClient)
	if err != nil {
		return errors.Wrap(err, "failed to init Kafka admin")
	}

	consumerGroup, err := sarama.NewConsumerGroupFromClient(envs.Kafka.Consumer.GroupID, kafkaClient)
	if err != nil {
		return errors.Wrapf(err, "failed to init Kafka consumer group %q", envs.Kafka.Consumer.GroupID)
	}

	defer func() {
		log.Info().Msgf("main: Closing Kafka consumer group %q", envs.Kafka.Consumer.GroupID)
		if err := consumerGroup.Close(); err != nil {
			log.Error().Stack().Err(err).Msg("main: Failed to close Kafka consumer group")
		}
	}()

	producer, err := sarama.NewSyncProducerFromClient(kafkaClient)
	if err != nil {
		return errors.Wrap(err, "failed to init Kafka producer")
	}

	defer func() {
		log.Info().Msg("main: Closing Kafka producer")
		if err := producer.Close(); err != nil {
			log.Error().Stack().Err(err).Msg("main: Failed to close Kafka producer")
		}
	}()

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

	srv := &http.Server{
		Addr:    envs.Web.Addr,
		Handler: rest.API(saramaAdmin, producer, envs.Gosuslugi.Host),
	}

	apiErr := make(chan error)
	go func() {
		log.Info().Msgf("main: Listening Validator service on %q", envs.Web.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			apiErr <- err
		}
	}()

	go func() {
		for err := range consumerGroup.Errors() {
			log.Error().Stack().Err(err).Msg("main: Consumer error")
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
			); err != nil && !errors.Is(err, sarama.ErrClosedConsumerGroup) {
				log.Fatal().Err(err).Msgf("main: Failed to consume from topic=%s", envs.Kafka.Topic.Name)
			}

			if ctx.Err() != nil {
				return
			}
		}
	}()

	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, syscall.SIGINT)

	select {
	case err := <-apiErr:
		return errors.WithStack(err)
	case sig := <-sigint:
		log.Info().Msgf("main: Received signal %q", sig)

		ctx, cancel := context.WithTimeout(context.Background(), envs.Web.ShutdownTimeout)
		defer cancel()

		log.Info().Msg("main: Doing server shutdown")
		if err := srv.Shutdown(ctx); err != nil {
			_ = srv.Close()
			return errors.Wrap(err, "failed to shutdown HTTP server")
		}
	}

	return nil
}
