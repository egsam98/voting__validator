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

var errGosuslugiCodes = map[int]struct{}{
	2: {},
}

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
func (v *VoterValidator) Run(ctx context.Context, vote *votingpb.Vote) error {
	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(request{
		Passport: vote.Voter.Passport,
	}); err != nil {
		return errors.Wrap(err, "failed to encode to JSON")
	}

	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, v.gosuslugiURL, buf)
	req.Header.Add("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "failed to validate voter")
	}

	defer func() {
		if err := res.Body.Close(); err != nil {
			log.Error().Stack().Err(err).Msg("failed to close HTTP response")
		}
	}()

	switch res.StatusCode {
	case http.StatusOK:
		updVoter := &votingpb.Voter{}
		if err := json.NewDecoder(res.Body).Decode(updVoter); err != nil {
			return errors.Wrapf(err, "failed to decode JSON to %T", updVoter)
		}

		switch {
		case updVoter.Fullname != vote.Voter.Fullname:
			vote.Status = votingpb.Vote_FAIL
			vote.FailReason = new(string)
			*vote.FailReason = "fullnames don't match"
		case updVoter.DeathDate != nil:
			vote.Status = votingpb.Vote_FAIL
			vote.FailReason = new(string)
			*vote.FailReason = fmt.Sprintf("voter died in %v", updVoter.DeathDate)
		default:
			vote.Voter = updVoter
		}

		return nil
	case http.StatusBadRequest:
		var errRes errResponse
		if err := json.NewDecoder(res.Body).Decode(&errRes); err != nil {
			return errors.Wrapf(err, "failed to decode JSON response to %T", errRes)
		}

		if _, ok := errGosuslugiCodes[errRes.Code]; !ok {
			return errors.Wrap(ErrInvalidInput, errRes.Error)
		}

		vote.Status = votingpb.Vote_FAIL
		vote.FailReason = &errRes.Error
		return nil
	default:
		return errors.Errorf("%s %d: internal Gosuslugi error", res.Status, res.StatusCode)
	}
}
