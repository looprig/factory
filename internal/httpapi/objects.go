package httpapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	sessionwire "github.com/looprig/core/sessionwire/v1"
	"github.com/looprig/factory/identity"
	internalidentity "github.com/looprig/factory/internal/identity"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"
)

// ObjectPolicy authorizes a reference using trusted committed session evidence.
// Metadata index existence is not that evidence. The returned kind comes from
// policy, never from a caller's reference syntax. Nil policy grants nothing.
// The catalog entry carries the immutable binding the evidence must belong to.
// A9 must supply the production policy; this interface does not implement one.
type ObjectPolicy interface {
	AuthorizeReference(context.Context, identity.Principal, sessionstore.CatalogEntry, sessionwire.ObjectReference) (sessionstore.ObjectKind, error)
}

// ObjectReader is the neutral read capability a frozen binding may resolve to.
// Streams must honor cancellation, permit concurrent Close with Read, and
// report whole-object integrity at EOF, as released SessionStore does.
type ObjectReader interface {
	GetObjectMetadata(context.Context, sessionstore.GetObjectMetadataRequest) (sessionwire.ObjectMetadata, error)
	GetObject(context.Context, sessionstore.GetObjectRequest) (io.ReadCloser, error)
}

// ObjectLimits bounds retained response memory and whole-object verification I/O.
// Hard ceilings bound configuration mistakes as well as caller requests.
type ObjectLimits struct{ MaxPageBytes, MaxVerificationBytes uint64 }

// DefaultObjectLimits retains at most 1 MiB per request and verifies at most
// 64 MiB. Larger artifacts need a different protocol. Paging rereads the whole
// object on each request; the route's RequestTimeout also bounds this work.
func DefaultObjectLimits() ObjectLimits { return ObjectLimits{1 << 20, 64 << 20} }
func (l ObjectLimits) Validate() error {
	if l.MaxPageBytes == 0 || l.MaxPageBytes > 1<<20 || l.MaxVerificationBytes < l.MaxPageBytes || l.MaxVerificationBytes > 64<<20 {
		return fmt.Errorf("%w: object limits must satisfy 0 < page <= 1 MiB and page <= verification <= 64 MiB", ErrInvalidRouterConfig)
	}
	return nil
}

func invalidObjectRequest() apiError {
	return apiError{status: 400, code: sessionwire.ErrorCodeInvalidRequest, message: "the object request is invalid"}
}
func objectUnavailable() apiError {
	return apiError{status: 503, code: ErrorCodeUnavailable, message: "this deployment cannot read the object at the moment"}
}
func objectTooLarge() apiError {
	return apiError{status: 413, code: ErrorCodePayloadTooLarge, message: "the object or requested page exceeds this deployment's read limit"}
}
func objectFailure(err error) apiError {
	if failure, ok := contextFailure(err); ok {
		return failure
	}
	if failure, ok := storeUnavailable(err); ok {
		return failure
	}
	var absent *storage.BlobNotFoundError
	var object *sessionstore.ObjectError
	if errors.As(err, &absent) {
		return apiError{status: 404, code: sessionwire.ErrorCodeInvalidRequest, message: "there is no readable object"}
	}
	if errors.As(err, &object) {
		switch object.Code {
		case sessionstore.ObjectErrorMetadataUnavailable:
			return apiError{status: 404, code: sessionwire.ErrorCodeInvalidRequest, message: "there is no readable object"}
		case sessionstore.ObjectErrorInvalid:
			return invalidObjectRequest()
		case sessionstore.ObjectErrorBackend:
			return objectUnavailable()
		}
	}
	return catalogFailure(err)
}

func (rt *Router) serveObject(metadataOnly bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		op, ok := internalidentity.OperationContextFrom(r.Context())
		if !ok {
			writeAPIError(w, internalFailure())
			return
		}
		entry, ok := resolvedSessionFrom(r.Context())
		if !ok {
			writeAPIError(w, internalFailure())
			return
		}
		if rt.objectPolicy == nil {
			writeAPIError(w, objectUnavailable())
			return
		}
		// Summary runs the released canonicalizer, including binding validation.
		// Zero binding is legacy only on a valid canonical legacy record.
		if _, err := entry.Record.Summary(); err != nil || entry.Record.TenantID != op.Principal.Tenant() || entry.Record.SessionID != sessionwire.SessionID(r.PathValue("sid")) {
			writeAPIError(w, internalFailure())
			return
		}
		ref := sessionwire.ObjectReference{ObjectID: r.PathValue("oid")}
		kind, err := rt.objectPolicy.AuthorizeReference(r.Context(), op.Principal, entry, ref)
		if err != nil {
			writeAPIError(w, authorizationFailure(err))
			return
		}
		reader := ObjectReader(rt.reads)
		var unbound sessionstore.SessionBinding
		if entry.Record.Binding != unbound {
			if rt.resolveObjectStore == nil {
				writeAPIError(w, objectUnavailable())
				return
			}
			reader, err = rt.resolveObjectStore(r.Context(), entry.Record.Binding)
			if err != nil {
				if fail, ok := contextFailure(err); ok {
					writeAPIError(w, fail)
				} else {
					writeAPIError(w, objectUnavailable())
				}
				return
			}
			if reader == nil {
				writeAPIError(w, objectUnavailable())
				return
			}
		}
		scope := newScope(op.Principal)
		m, err := reader.GetObjectMetadata(r.Context(), scope.objectMetadata(entry.Record.SessionID, ref, kind))
		if err != nil {
			writeAPIError(w, objectFailure(err))
			return
		}
		if err := m.Validate(); err != nil || m.Reference != ref {
			writeAPIError(w, internalFailure())
			return
		}
		if metadataOnly {
			body, err := json.Marshal(m)
			if err != nil {
				writeAPIError(w, internalFailure())
				return
			}
			writeJSONBytes(w, 200, body)
			return
		}
		start, end, partial, fail := objectRange(r.Header.Values("Range"), m.SizeBytes, rt.objectLimits.MaxPageBytes)
		if fail.status != 0 {
			if fail.status == 416 {
				w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", m.SizeBytes))
			}
			writeAPIError(w, fail)
			return
		}
		if m.SizeBytes > rt.objectLimits.MaxVerificationBytes {
			writeAPIError(w, objectTooLarge())
			return
		}
		stream, err := reader.GetObject(r.Context(), scope.objectBody(entry.Record.SessionID, m, kind))
		if err != nil {
			writeAPIError(w, objectFailure(err))
			return
		}
		if stream == nil {
			writeAPIError(w, internalFailure())
			return
		}
		page, err := verifiedObjectPage(r.Context(), stream, m, start, end, rt.objectLimits.MaxVerificationBytes)
		if err != nil {
			writeAPIError(w, objectFailure(err))
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(page)))
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("X-Object-Digest", m.Digest)
		w.Header().Set("X-Object-Size", strconv.FormatUint(m.SizeBytes, 10))
		status := 200
		if partial {
			status = 206
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, m.SizeBytes))
		}
		w.WriteHeader(status)
		if r.Method != http.MethodHead {
			_, _ = w.Write(page)
		}
	})
}

// objectRange implements exact pages: no clipping, suffix, open or multipart
// ranges. An inclusive end beyond EOF is unsatisfiable, not a shorter page.
func objectRange(values []string, size, maxPage uint64) (start, end uint64, partial bool, failure apiError) {
	if len(values) == 0 {
		if size > maxPage {
			return 0, 0, false, objectTooLarge()
		}
		if size > 0 {
			end = size - 1
		}
		return 0, end, false, apiError{}
	}
	if len(values) != 1 || !strings.HasPrefix(values[0], "bytes=") {
		return 0, 0, false, invalidObjectRequest()
	}
	a, b, ok := strings.Cut(strings.TrimPrefix(values[0], "bytes="), "-")
	digits := func(s string) bool {
		if s == "" {
			return false
		}
		for _, c := range s {
			if c < '0' || c > '9' {
				return false
			}
		}
		return true
	}
	if !ok || !digits(a) || !digits(b) {
		return 0, 0, false, invalidObjectRequest()
	}
	start, err := strconv.ParseUint(a, 10, 64)
	if err != nil {
		return 0, 0, false, invalidObjectRequest()
	}
	end, err = strconv.ParseUint(b, 10, 64)
	if err != nil || end < start {
		return 0, 0, false, invalidObjectRequest()
	}
	if start >= size || end >= size {
		return 0, 0, false, apiError{status: 416, code: sessionwire.ErrorCodeInvalidRequest, message: "the byte range is outside the object"}
	}
	if end-start >= maxPage {
		return 0, 0, false, objectTooLarge()
	}
	return start, end, true, apiError{}
}

// verifiedObjectPage retains only the page and drains the entire bounded object
// before emitting bytes. Independent digest/size checks defend the narrow seam.
// Close completes before success. Cancellation closes a blocked provider Read.
func verifiedObjectPage(ctx context.Context, stream io.ReadCloser, m sessionwire.ObjectMetadata, start, end, ceiling uint64) (page []byte, err error) {
	closed := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = stream.Close(); close(closed) })
	defer func() {
		if stop() {
			closeErr := stream.Close()
			if err == nil {
				err = closeErr
			}
		} else {
			<-closed
		}
		if ctx.Err() != nil {
			err = ctx.Err()
		}
	}()
	count := uint64(0)
	if m.SizeBytes > 0 {
		count = end - start + 1
	}
	page = make([]byte, int(count))
	buf := make([]byte, 32<<10)
	digest := sha256.New()
	var offset uint64
	emptyReads := 0
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// One extra byte distinguishes genuine EOF from a prefix ending exactly
		// at the ceiling. Actual provider reads are bounded by ceiling plus one.
		remaining := ceiling - offset + 1
		window := buf
		if remaining < uint64(len(window)) {
			window = window[:remaining]
		}
		n, readErr := stream.Read(window)
		if n < 0 || n > len(window) {
			return nil, errors.New("invalid object reader")
		}
		if n > 0 {
			emptyReads = 0
			if uint64(n) > ceiling-offset || offset+uint64(n) > m.SizeBytes {
				return nil, errors.New("object size mismatch")
			}
			_, _ = digest.Write(window[:n])
			next := offset + uint64(n)
			left, right := max(offset, start), min(next, end+1)
			if left < right && count > 0 {
				copy(page[left-start:right-start], window[left-offset:right-offset])
			}
			offset = next
		} else {
			emptyReads++
			if emptyReads > 100 {
				return nil, io.ErrNoProgress
			}
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				return nil, readErr
			}
			break
		}
	}
	if offset != m.SizeBytes || m.Digest != "sha256:"+hex.EncodeToString(digest.Sum(nil)) {
		return nil, errors.New("object integrity mismatch")
	}
	return page, nil
}
