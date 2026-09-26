package command

import (
	"context"
	"errors"
	"fmt"
	"time"

	"connectrpc.com/connect"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
	"github.com/7K-Inari/inari-api/gen/go/inari/agent/v1/agentv1connect"

	"github.com/7K-Inari/inari-agent/internal/stream"
)

// RedeemCode classifies RedeemUserCredential failures. Missing, expired,
// already-redeemed, invalid, and denied refs are terminal command failures
// (FAILED_PRECONDITION style): the control plane must issue a fresh command
// with a fresh ref. Anything else is transient and follows the dispatcher's
// normal redelivery path.
type RedeemCode string

const (
	RedeemNotFound        RedeemCode = "not_found"
	RedeemExpired         RedeemCode = "expired"
	RedeemAlreadyRedeemed RedeemCode = "already_redeemed"
	RedeemInvalid         RedeemCode = "invalid"
	RedeemDenied          RedeemCode = "denied"
)

// RedeemError is a typed, terminal credential-redemption failure. The Error
// string is a fixed generic message per code: server error details are never
// echoed, so no token or vault material can leak into acks or logs.
type RedeemError struct {
	Code RedeemCode
}

func (e *RedeemError) Error() string {
	switch e.Code {
	case RedeemNotFound:
		return "user credential not found or revoked"
	case RedeemExpired:
		return "user credential expired"
	case RedeemAlreadyRedeemed:
		return "user credential already redeemed"
	case RedeemInvalid:
		return "user credential reference invalid"
	case RedeemDenied:
		return "user credential redemption denied"
	default:
		return "user credential redemption failed"
	}
}

// Terminal reports whether the failure must not be retried. RedeemError is
// always terminal; a client-side retry loop could mask revoked credentials.
func (e *RedeemError) Terminal() bool { return true }

// UserCredentialRedeemer exchanges an opaque user_credential_ref for a
// short-lived ArgoCD bearer at the control plane. Implementations are bound
// to the agent's cluster identity; returned tokens live in memory only.
type UserCredentialRedeemer interface {
	Redeem(ctx context.Context, ref string) (token string, expiresAt time.Time, err error)
}

// ConnectRedeemer is the production UserCredentialRedeemer backed by the
// agent.v1 AgentCredentialsService (ConnectRPC), authenticated with the
// agent's cluster-identity OIDC JWT — the same token source and gateway
// address as the event stream.
type ConnectRedeemer struct {
	client agentv1connect.AgentCredentialsServiceClient
}

// NewConnectRedeemer builds a redeemer for the given control-plane base URL.
// Token supplies the agent's short-lived cluster JWT per call; httpClient
// may be nil (defaults to the stream's proxy-aware HTTP client).
func NewConnectRedeemer(address string, token stream.TokenSource, httpClient connect.HTTPClient) *ConnectRedeemer {
	if httpClient == nil {
		httpClient = stream.DefaultHTTPClient(address)
	}
	auth := connect.WithInterceptors(connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			tok, err := token.Token(ctx)
			if err != nil {
				return nil, fmt.Errorf("redeem: fetch agent token: %w", err)
			}
			req.Header().Set("Authorization", "Bearer "+tok)
			return next(ctx, req)
		}
	}))
	return &ConnectRedeemer{
		client: agentv1connect.NewAgentCredentialsServiceClient(httpClient, address, auth),
	}
}

// Redeem implements UserCredentialRedeemer. Exactly one RPC per call: no
// retry loop, so a revoked credential can never be masked by re-redemption.
func (r *ConnectRedeemer) Redeem(ctx context.Context, ref string) (string, time.Time, error) {
	resp, err := r.client.RedeemUserCredential(ctx, connect.NewRequest(&agentv1.RedeemUserCredentialRequest{
		CredentialRef: ref,
	}))
	if err != nil {
		return "", time.Time{}, classifyRedeemError(err)
	}
	var exp time.Time
	if resp.Msg.ExpiresAt != nil {
		exp = resp.Msg.ExpiresAt.AsTime()
	}
	return resp.Msg.BearerToken, exp, nil
}

// classifyRedeemError maps Connect codes to typed terminal RedeemErrors.
// Unknown/unavailable codes pass through unchanged (transient). Server
// messages are deliberately discarded so no token material is echoed.
func classifyRedeemError(err error) error {
	var ce *connect.Error
	if !errors.As(err, &ce) {
		return err
	}
	switch ce.Code() {
	case connect.CodeNotFound:
		return &RedeemError{Code: RedeemNotFound}
	case connect.CodeFailedPrecondition:
		return &RedeemError{Code: RedeemExpired}
	case connect.CodeAlreadyExists:
		return &RedeemError{Code: RedeemAlreadyRedeemed}
	case connect.CodeInvalidArgument:
		return &RedeemError{Code: RedeemInvalid}
	case connect.CodePermissionDenied, connect.CodeUnauthenticated:
		return &RedeemError{Code: RedeemDenied}
	default:
		return err
	}
}
