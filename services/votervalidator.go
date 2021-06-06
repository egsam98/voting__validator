package services

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	votingpb "github.com/egsam98/voting/proto"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

// VoterValidator validates voter by using Gosuslugi's external API
type VoterValidator struct {
	gosuslugiURL string
}

func NewVoterValidator(gosuslugiHost string) *VoterValidator {
	url := fmt.Sprintf("http://%s/api/validate", gosuslugiHost)
	return &VoterValidator{gosuslugiURL: url}
}

// Run starts vote validation using Gogsuslugi
func (v *VoterValidator) Run(ctx context.Context, vote *votingpb.Vote) error {
	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(vote); err != nil {
		return errors.Wrapf(err, "failed to encode vote %#v to JSON", vote)
	}

	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, v.gosuslugiURL, buf)
	req.Header.Add("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "failed to validate vote")
	}

	defer func() {
		if err := res.Body.Close(); err != nil {
			log.Error().Stack().Err(err).Msg("failed to close HTTP response")
		}
	}()

	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res.Body)
		return errors.Errorf("invalid vote, JSON response: %s", string(b))
	}

	return nil
}
