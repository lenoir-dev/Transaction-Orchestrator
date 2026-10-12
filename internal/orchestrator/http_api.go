package orchestrator

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type HTTPConfig struct {
	Store            *Store
	Runtime          *Runtime
	Endpoints        *EndpointPool
	Signer           func(AccountKey) TransactionSigner
	Tokens           map[string]string // bearer token to tenant ID
	Accounts         []AccountKey
	Policies         map[string]Policy
	Fees             map[string]FeeInput
	Signing          SignOptions
	NonceReadTimeout time.Duration
	RequestTimeout   time.Duration
	MaxBodyBytes     int64
	MaxCalldataBytes int
	MaxBacklog       int64
	RuntimeReady     func() bool
	Logger           *slog.Logger
	Version          string
	SourceIdentity   string
}

type HTTPAPI struct {
	cfg      HTTPConfig
	mux      *http.ServeMux
	mu       sync.Mutex
	locks    [64]sync.Mutex
	accounts [64]sync.Mutex
	created  uint64
	replayed uint64
}

type APIError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}

type apiCreateBody struct {
	ChainID   string `json:"chain_id"`
	SignerID  string `json:"signer_id"`
	To        string `json:"to"`
	ValueWei  string `json:"value_wei"`
	Calldata  string `json:"calldata"`
	GasPolicy struct {
		Profile  string `json:"profile"`
		GasLimit uint64 `json:"gas_limit,omitempty"`
	} `json:"gas_policy"`
	ConfirmationDepth int `json:"confirmation_depth,omitempty"`
}

var requestIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

func NewHTTPAPI(cfg HTTPConfig) (*HTTPAPI, error) {
	if cfg.Store == nil || cfg.Runtime == nil || cfg.Endpoints == nil || cfg.Signer == nil ||
		len(cfg.Tokens) == 0 || len(cfg.Accounts) == 0 || len(cfg.Policies) == 0 ||
		cfg.Signing.SignerTimeout <= 0 || cfg.Signing.LeaseDuration <= cfg.Signing.SignerTimeout+time.Second ||
		cfg.NonceReadTimeout <= 0 || cfg.NonceReadTimeout > time.Minute ||
		cfg.RequestTimeout < time.Second || cfg.RequestTimeout > time.Minute ||
		cfg.MaxBodyBytes < 256 || cfg.MaxBodyBytes > 1<<20 ||
		cfg.MaxCalldataBytes < 0 || cfg.MaxCalldataBytes > 1<<19 ||
		cfg.MaxBacklog < 1 || cfg.RuntimeReady == nil {
		return nil, ErrInvalid
	}
	for token, tenant := range cfg.Tokens {
		if len(token) < 16 || len(token) > 256 || len(tenant) < 1 || len(tenant) > 64 ||
			strings.TrimSpace(tenant) != tenant {
			return nil, ErrInvalid
		}
	}
	for profile, policy := range cfg.Policies {
		if profile != "standard" && profile != "cautious" {
			return nil, ErrInvalid
		}
		if _, _, err := policy.validate(); err != nil {
			return nil, ErrInvalid
		}
		if cfg.Fees[profile].MaxFeePerGasWei == "" || cfg.Fees[profile].MaxTipPerGasWei == "" {
			return nil, ErrInvalid
		}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	a := &HTTPAPI{cfg: cfg, mux: http.NewServeMux()}
	a.mux.HandleFunc("POST /v1/transactions", a.create)
	a.mux.HandleFunc("GET /v1/transactions/{id}", a.get)
	a.mux.HandleFunc("GET /v1/status", a.status)
	a.mux.HandleFunc("GET /health/live", a.live)
	a.mux.HandleFunc("GET /health/ready", a.ready)
	a.mux.HandleFunc("GET /metrics", a.metrics)
	return a, nil
}

func (a *HTTPAPI) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")
		id := r.Header.Get("X-Request-ID")
		if !requestIDPattern.MatchString(id) {
			var bytes [16]byte
			if _, err := rand.Read(bytes[:]); err != nil {
				a.writeError(w, "", http.StatusServiceUnavailable, "REQUEST_ID_UNAVAILABLE", "Request unavailable")
				return
			}
			id = hex.EncodeToString(bytes[:])
		}
		w.Header().Set("X-Request-ID", id)
		ctx, cancel := context.WithTimeout(r.Context(), a.cfg.RequestTimeout)
		defer cancel()
		route := r.URL.Path
		if strings.HasPrefix(route, "/v1/transactions/") {
			route = "/v1/transactions/{id}"
		}
		ctx, span := otel.Tracer("transaction-orchestrator").Start(ctx, "http.request",
			trace.WithAttributes(attribute.String("http.route", route), attribute.String("http.method", r.Method)))
		defer span.End()
		a.mux.ServeHTTP(w, r.WithContext(context.WithValue(ctx, requestIDKey{}, id)))
	})
}

type requestIDKey struct{}

func requestID(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

func (a *HTTPAPI) tenant(r *http.Request) (string, bool) {
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, "Bearer ") || len(header) > 300 {
		return "", false
	}
	provided := strings.TrimPrefix(header, "Bearer ")
	for token, tenant := range a.cfg.Tokens {
		if len(token) == len(provided) && subtle.ConstantTimeCompare([]byte(token), []byte(provided)) == 1 {
			return tenant, true
		}
	}
	return "", false
}

func (a *HTTPAPI) writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func (a *HTTPAPI) writeError(w http.ResponseWriter, id string, status int, code, message string) {
	a.writeJSON(w, status, APIError{Code: code, Message: message, RequestID: id})
}

func (a *HTTPAPI) failure(w http.ResponseWriter, r *http.Request, err error) {
	id := requestID(r.Context())
	switch {
	case errors.Is(err, ErrConflict):
		a.writeError(w, id, http.StatusConflict, "IDEMPOTENCY_CONFLICT", "Idempotency key conflicts with an existing intent")
	case errors.Is(err, ErrNotFound):
		a.writeError(w, id, http.StatusNotFound, "NOT_FOUND", "Intent not found")
	case errors.Is(err, ErrInvalid):
		a.writeError(w, id, http.StatusUnprocessableEntity, "INVALID_REQUEST", "Request violates transaction policy")
	case errors.Is(err, ErrAccountConflict), errors.Is(err, ErrAccountPaused):
		a.writeError(w, id, http.StatusServiceUnavailable, "ACCOUNT_UNAVAILABLE", "Configured signer account is unavailable")
	case errors.Is(err, context.DeadlineExceeded):
		a.writeError(w, id, http.StatusGatewayTimeout, "TIMEOUT", "Request deadline exceeded")
	default:
		a.cfg.Logger.Error("api operation failed", "request_id", id, "error_type", fmt.Sprintf("%T", err))
		a.writeError(w, id, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "Operation unavailable")
	}
}

func (a *HTTPAPI) accountAllowed(key AccountKey) bool {
	for _, allowed := range a.cfg.Accounts {
		if allowed == key {
			return true
		}
	}
	return false
}

func (a *HTTPAPI) admissionReady(ctx context.Context, key AccountKey) error {
	if !a.cfg.RuntimeReady() || !a.accountAllowed(key) {
		return ErrAccountPaused
	}
	if len(a.cfg.Endpoints.Eligible(key.ChainID, EndpointRead|EndpointBroadcast|EndpointNonce)) == 0 {
		return ErrNoEligibleEndpoint
	}
	account, err := a.cfg.Store.AccountStatus(ctx, key)
	if err != nil {
		return err
	}
	if account.State != AccountReady || account.PausedReason != "" || account.SignerStatus != "AVAILABLE" {
		return ErrAccountPaused
	}
	status, err := a.cfg.Runtime.Status(ctx)
	if err != nil {
		return err
	}
	var backlog int64
	for _, count := range status.Backlog {
		backlog += count
	}
	if backlog >= a.cfg.MaxBacklog {
		return ErrAccountPaused
	}
	return nil
}

func (a *HTTPAPI) create(w http.ResponseWriter, r *http.Request) {
	tenant, ok := a.tenant(r)
	if !ok {
		a.writeError(w, requestID(r.Context()), http.StatusUnauthorized, "UNAUTHORIZED", "Authentication required")
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if len(key) < 1 || len(key) > 255 || strings.TrimSpace(key) != key {
		a.writeError(w, requestID(r.Context()), http.StatusBadRequest, "INVALID_IDEMPOTENCY_KEY", "Invalid Idempotency-Key")
		return
	}
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		a.writeError(w, requestID(r.Context()), http.StatusUnsupportedMediaType, "UNSUPPORTED_MEDIA_TYPE", "Use application/json")
		return
	}
	data, err := io.ReadAll(io.LimitReader(r.Body, a.cfg.MaxBodyBytes+1))
	if err != nil {
		a.writeError(w, requestID(r.Context()), http.StatusBadRequest, "INVALID_JSON", "Could not read request")
		return
	}
	if int64(len(data)) > a.cfg.MaxBodyBytes {
		a.writeError(w, requestID(r.Context()), http.StatusRequestEntityTooLarge, "BODY_TOO_LARGE", "Request body exceeds limit")
		return
	}
	var body apiCreateBody
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&body); err != nil || decoder.Decode(new(any)) != io.EOF {
		a.writeError(w, requestID(r.Context()), http.StatusBadRequest, "INVALID_JSON", "Malformed JSON request")
		return
	}
	if len(body.Calldata) > 2+a.cfg.MaxCalldataBytes*2 {
		a.writeError(w, requestID(r.Context()), http.StatusRequestEntityTooLarge, "CALLDATA_TOO_LARGE", "Calldata exceeds limit")
		return
	}
	policy, ok := a.cfg.Policies[body.GasPolicy.Profile]
	if !ok || (body.GasPolicy.GasLimit != 0 && body.GasPolicy.GasLimit != policy.GasLimit) ||
		(body.ConfirmationDepth != 0 && body.ConfirmationDepth != policy.ConfirmationDepth) {
		a.writeError(w, requestID(r.Context()), http.StatusUnprocessableEntity, "POLICY_DENIED", "Configured policy does not permit requested values")
		return
	}
	req := Request{TenantID: tenant, IdempotencyKey: key, ChainID: body.ChainID,
		SignerID: body.SignerID, To: body.To, ValueWei: body.ValueWei,
		Calldata: body.Calldata, GasProfile: body.GasPolicy.Profile,
		GasLimit: body.GasPolicy.GasLimit, ConfirmationDepth: body.ConfirmationDepth}
	normalized, err := normalize(req)
	if err != nil {
		a.failure(w, r, err)
		return
	}
	req.ChainID = normalized.chainID
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(tenant))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(key))
	lock := &a.locks[hash.Sum64()%uint64(len(a.locks))]
	lock.Lock()
	defer lock.Unlock()
	existing, lookupErr := a.cfg.Store.ExistingIntent(r.Context(), req)
	if lookupErr == nil {
		view, err := a.cfg.Store.IntentView(r.Context(), existing.ID, tenant)
		if err != nil {
			a.failure(w, r, err)
			return
		}
		a.mu.Lock()
		a.replayed++
		a.mu.Unlock()
		a.writeJSON(w, http.StatusOK, view)
		return
	}
	if !errors.Is(lookupErr, ErrNotFound) {
		a.failure(w, r, lookupErr)
		return
	}
	accountKey := AccountKey{ChainID: req.ChainID, SignerID: req.SignerID}
	accountHash := fnv.New64a()
	_, _ = accountHash.Write([]byte(req.ChainID))
	_, _ = accountHash.Write([]byte{0})
	_, _ = accountHash.Write([]byte(req.SignerID))
	accountLock := &a.accounts[accountHash.Sum64()%uint64(len(a.accounts))]
	accountLock.Lock()
	defer accountLock.Unlock()
	if err = a.admissionReady(r.Context(), accountKey); err != nil {
		a.failure(w, r, err)
		return
	}
	admitCtx, admitSpan := otel.Tracer("transaction-orchestrator").Start(r.Context(), "db.admission")
	intent, created, err := a.cfg.Store.CreateIntentWithResult(admitCtx, req, policy)
	admitSpan.End()
	if err != nil {
		a.failure(w, r, err)
		return
	}
	if intent.State == Created {
		var nonce uint64
		if nonce, err = a.cfg.Store.ReserveReconciledNonce(r.Context(), intent.ID,
			a.cfg.Endpoints, a.cfg.NonceReadTimeout); err != nil {
			a.failure(w, r, err)
			return
		}
		a.cfg.Logger.Info("nonce reserved", "request_id", requestID(r.Context()),
			"intent_id", intent.ID, "nonce", nonce)
	}
	if intent.State == Created || intent.State == Reserved || intent.State == Signing {
		signer := a.cfg.Signer(accountKey)
		if signer == nil {
			a.failure(w, r, ErrAccountPaused)
			return
		}
		var signed AttemptIdentity
		signed, err = a.cfg.Store.SignInitial(r.Context(), intent.ID, signer,
			a.cfg.Fees[body.GasPolicy.Profile], a.cfg.Signing)
		if err != nil {
			a.failure(w, r, err)
			return
		}
		a.cfg.Logger.Info("attempt signed", "request_id", requestID(r.Context()),
			"intent_id", intent.ID, "attempt_number", signed.AttemptNumber,
			"transaction_hash", signed.Hash.Hex())
	}
	view, err := a.cfg.Store.IntentView(r.Context(), intent.ID, tenant)
	if err != nil {
		a.failure(w, r, err)
		return
	}
	a.mu.Lock()
	if created {
		a.created++
	} else {
		a.replayed++
	}
	a.mu.Unlock()
	status := http.StatusOK
	if created {
		status = http.StatusCreated
		w.Header().Set("Location", "/v1/transactions/"+intent.ID)
		a.cfg.Logger.Info("intent created", "request_id", requestID(r.Context()),
			"intent_id", intent.ID, "chain_id", intent.ChainID, "signer_id", intent.SignerID)
	}
	a.writeJSON(w, status, view)
}

func (a *HTTPAPI) get(w http.ResponseWriter, r *http.Request) {
	tenant, ok := a.tenant(r)
	if !ok {
		a.writeError(w, requestID(r.Context()), http.StatusUnauthorized, "UNAUTHORIZED", "Authentication required")
		return
	}
	id := r.PathValue("id")
	if len(id) != 36 || !validUUID(id) {
		a.writeError(w, requestID(r.Context()), http.StatusBadRequest, "INVALID_ID", "Invalid intent ID")
		return
	}
	view, err := a.cfg.Store.IntentView(r.Context(), id, tenant)
	if err != nil {
		a.failure(w, r, err)
		return
	}
	a.writeJSON(w, http.StatusOK, view)
}

func validUUID(value string) bool {
	for i, c := range value {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
			continue
		}
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}
