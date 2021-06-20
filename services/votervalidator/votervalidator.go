package votervalidator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	votingpb "github.com/egsam98/voting/proto"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

const errCodeUserNotFound = 2

// VoterValidator validates voter by using Gosuslugi's external API
type VoterValidator struct {
	gosuslugiURL string
}

type request struct {
	Passport string `json:"passport"`
}

type errResponse struct {
	Error string `json:"error"`
	Code  int    `json:"code"`
}

func New(gosuslugiHost string) *VoterValidator {
	url := fmt.Sprintf("http://%s/api/users/passport", gosuslugiHost)
	return &VoterValidator{gosuslugiURL: url}
}

// Run starts vote validation using Gogsuslugi
func (v *VoterValidator) Run(ctx context.Context, voter *votingpb.Voter) (*votingpb.Voter, error) {
	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(request{
		Passport: voter.Passport,
	}); err != nil {
		return nil, errors.Wrap(err, "failed to encode to JSON")
	}

	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, v.gosuslugiURL, buf)
	req.Header.Add("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to validate voter")
	}

	defer func() {
		if err := res.Body.Close(); err != nil {
			log.Error().Stack().Err(err).Msg("failed to close HTTP response")
		}
	}()

	if res.StatusCode == http.StatusOK {
		return nil, nil
	}

	if res.StatusCode == http.StatusBadRequest {
		var errRes errResponse
		if err := json.NewDecoder(res.Body).Decode(&errRes); err != nil {
			return nil, errors.Wrapf(err, "failed to decode JSON response to %T", errRes)
		}

		if errRes.Code == errCodeUserNotFound {
			return nil, errors.Wrap(ErrInvalidVoter, errRes.Error)
		}

		return nil, errors.Wrap(ErrInvalidInput, errRes.Error)
	}

	updVoter := &votingpb.Voter{}
	if err := json.NewDecoder(res.Body).Decode(updVoter); err != nil {
		return nil, errors.Wrapf(err, "failed to decode JSON to %T", updVoter)
	}

	if updVoter.Fullname != voter.Fullname {
		return nil, errors.Wrap(ErrInvalidVoter, "fullnames don't match")
	}

	if updVoter.DeathDate != nil {
		return nil, errors.Wrapf(ErrInvalidVoter, "voter died in %v", updVoter.DeathDate)
	}

	return updVoter, nil
}
