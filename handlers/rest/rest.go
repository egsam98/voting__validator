package rest

import (
	"net/http"

	"github.com/Shopify/sarama"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/rs/zerolog/log"
)

func API(admin sarama.ClusterAdmin, prod sarama.SyncProducer, gosuslugiHost string) http.Handler {
	hc := newHealthController(admin, prod, gosuslugiHost)

	mux := chi.NewMux()
	mux.Use(
		middleware.Recoverer,
		middleware.RequestLogger(&middleware.DefaultLogFormatter{
			Logger: &log.Logger,
		}),
	)

	mux.Route("/health", func(r chi.Router) {
		r.Get("/readiness", hc.Readiness)
	})

	return mux
}
