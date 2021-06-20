package rest

import (
	"errors"
	"io"
	"net/http"

	"github.com/Shopify/sarama"
	"github.com/go-chi/render"
)

const healthTopic = "health"

type healthController struct {
	admin         sarama.ClusterAdmin
	prod          sarama.SyncProducer
	gosuslugiHost string
}

func newHealthController(
	admin sarama.ClusterAdmin,
	prod sarama.SyncProducer,
	gosuslugiHost string,
) *healthController {
	return &healthController{
		admin:         admin,
		prod:          prod,
		gosuslugiHost: gosuslugiHost,
	}
}

type status struct {
	Status string `json:"status"`
}

func fail(w http.ResponseWriter, r *http.Request, serviceName string, err error) {
	w.WriteHeader(http.StatusInternalServerError)
	render.JSON(w, r, status{Status: serviceName + ": " + err.Error()})
}

func ok(w http.ResponseWriter, r *http.Request) {
	render.JSON(w, r, status{Status: "ok"})
}

func (hc *healthController) Readiness(w http.ResponseWriter, r *http.Request) {
	if err := hc.admin.CreateTopic(healthTopic, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false); err != nil {
		fail(w, r, "kafka admin", err)
		return
	}

	if _, _, err := hc.prod.SendMessage(&sarama.ProducerMessage{Topic: healthTopic}); err != nil {
		fail(w, r, "kafka", err)
		return
	}

	if err := hc.admin.DeleteTopic(healthTopic); err != nil {
		fail(w, r, "kafka admin", err)
		return
	}

	res, err := http.Get(hc.gosuslugiHost + "/health/readiness")
	if err != nil {
		fail(w, r, "gosuslugi", err)
		return
	}

	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res.Body)
		fail(w, r, "gosuslugi", errors.New(string(b)))
		return
	}

	ok(w, r)
}
