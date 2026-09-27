package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/application"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/clock"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/domain"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/httpapi"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/idgen"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/observability"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/storage"
)

type testFixture struct {
	server *httptest.Server
}

type rangeView struct {
	BaseOffset int64 `json:"base_offset"`
	EndOffset  int64 `json:"end_offset"`
}

type segmentView struct {
	ID       string    `json:"id"`
	StreamID string    `json:"stream_id"`
	State    string    `json:"state"`
	Range    rangeView `json:"range"`
}

type segmentListBody struct {
	Items []segmentView `json:"items"`
}

type errorBodyView struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details"`
}

type errorEnvelope struct {
	Error errorBodyView `json:"error"`
}

type createStreamBody struct {
	Stream struct {
		ID string `json:"id"`
	} `json:"stream"`
	Segment struct {
		ID string `json:"id"`
	} `json:"segment"`
}

func newTestFixture(t *testing.T) *testFixture {
	t.Helper()
	repo := storage.NewInMemoryRepository("test://state", nil)
	service, err := application.NewService(
		repo,
		&idgen.Counter{},
		clock.NewFixed(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Second),
		observability.Discard{},
		application.Limits{MaxAppendRecords: 100, MaxAppendBytes: 1 << 20},
	)
	if err != nil {
		t.Fatalf("create service: %v", err)
	}
	api := httpapi.New(service, httpapi.Options{
		MaxRequestBytes: 1 << 20,
		RequestTimeout:  3 * time.Second,
		Logger:          observability.Discard{},
	})
	server := httptest.NewServer(api.Handler())
	t.Cleanup(server.Close)
	return &testFixture{server: server}
}

func readAll(t *testing.T, response *http.Response) []byte {
	t.Helper()
	data, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return data
}

func mustDecode(t *testing.T, data []byte, target any) {
	t.Helper()
	if err := json.Unmarshal(data, target); err != nil {
		t.Fatalf("decode %s: %v", string(data), err)
	}
}

func (f *testFixture) do(
	t *testing.T,
	method string,
	path string,
	body any,
) (int, []byte) {
	t.Helper()
	var reader io.Reader = http.NoBody
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("encode body: %v", err)
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(context.Background(), method, f.server.URL+path, reader)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("perform request %s %s: %v", method, path, err)
	}
	defer response.Body.Close()
	return response.StatusCode, readAll(t, response)
}

// createStreamWithSegments registers a stream whose threshold is reached by
// every append, so each append seals the active segment and opens the next
// one. The returned stream owns appends+1 segments ordered by base offset with
// the final segment open and empty.
func (f *testFixture) createStreamWithSegments(
	t *testing.T,
	namespace string,
	appends int,
) (string, []segmentView) {
	t.Helper()
	status, data := f.do(t, http.MethodPost, "/v1/streams", map[string]any{
		"namespace":       namespace,
		"name":            "events-" + namespace,
		"threshold_bytes": 100,
	})
	if status != http.StatusCreated {
		t.Fatalf("create stream status = %d body = %s", status, data)
	}
	var created createStreamBody
	mustDecode(t, data, &created)
	streamID := created.Stream.ID

	revision := uint64(1)
	for index := 0; index < appends; index++ {
		status, data = f.do(t, http.MethodPost,
			"/v1/streams/"+streamID+"/records",
			map[string]any{
				"expected_revision": revision,
				"record_count":      1,
				"payload_bytes":     100,
			},
		)
		if status != http.StatusOK {
			t.Fatalf("append %d status = %d body = %s", index, status, data)
		}
		revision++
	}

	status, data = f.do(t, http.MethodGet,
		"/v1/streams/"+streamID+"/segments?limit=1000", nil)
	if status != http.StatusOK {
		t.Fatalf("list segments status = %d body = %s", status, data)
	}
	var listed segmentListBody
	mustDecode(t, data, &listed)
	if len(listed.Items) != appends+1 {
		t.Fatalf("expected %d segments, got %d", appends+1, len(listed.Items))
	}
	return streamID, listed.Items
}

func listOK(t *testing.T, status int, data []byte) segmentListBody {
	t.Helper()
	if status != http.StatusOK {
		t.Fatalf("status = %d body = %s, want 200", status, data)
	}
	var body segmentListBody
	mustDecode(t, data, &body)
	return body
}

func expectInvalidCursor(t *testing.T, status int, data []byte) errorBodyView {
	t.Helper()
	if status != http.StatusBadRequest {
		t.Fatalf("status = %d body = %s, want 400", status, data)
	}
	var envelope errorEnvelope
	mustDecode(t, data, &envelope)
	if envelope.Error.Code != string(domain.CodeInvalidCursor) {
		t.Fatalf("error code = %q, want %q", envelope.Error.Code, domain.CodeInvalidCursor)
	}
	return envelope.Error
}

func segmentIDs(items []segmentView) []string {
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.ID)
	}
	return ids
}

func expectBases(t *testing.T, items []segmentView, want []int64) {
	t.Helper()
	if len(items) != len(want) {
		t.Fatalf("got %d segments (%v), want %d", len(items), bases(items), len(want))
	}
	for index, base := range want {
		if items[index].Range.BaseOffset != base {
			t.Fatalf("segment %d base = %d, want %d (order %v)",
				index, items[index].Range.BaseOffset, base, bases(items))
		}
	}
}

func bases(items []segmentView) []int64 {
	values := make([]int64, 0, len(items))
	for _, item := range items {
		values = append(values, item.Range.BaseOffset)
	}
	return values
}

func TestListSegmentsPaginatesInStableOrder(t *testing.T) {
	fixture := newTestFixture(t)
	streamID, all := fixture.createStreamWithSegments(t, "alpha", 4)
	if len(all) != 5 {
		t.Fatalf("expected five segments, got %d", len(all))
	}

	// Page through with limit=2 using the last returned identifier as the cursor.
	var collected []segmentView
	after := ""
	for page := 0; ; page++ {
		path := fmt.Sprintf("/v1/streams/%s/segments?limit=2", streamID)
		if after != "" {
			path += "&after=" + after
		}
		status, data := fixture.do(t, http.MethodGet, path, nil)
		body := listOK(t, status, data)

		switch page {
		case 0, 1:
			if len(body.Items) != 2 {
				t.Fatalf("page %d size = %d, want 2", page, len(body.Items))
			}
		case 2:
			// Last page holds the single open tail segment.
			if len(body.Items) != 1 {
				t.Fatalf("last page size = %d, want 1", len(body.Items))
			}
		default:
			t.Fatalf("pagination produced an unexpected page %d: %v", page, segmentIDs(body.Items))
		}
		collected = append(collected, body.Items...)

		// The cursor returned for the final item of a full page resolves to the
		// remaining items without skipping or repeating anything.
		after = body.Items[len(body.Items)-1].ID
		if page == 2 {
			break
		}
	}

	if len(collected) != len(all) {
		t.Fatalf("paged total = %d, want %d", len(collected), len(all))
	}
	for index := range all {
		if collected[index].ID != all[index].ID {
			t.Fatalf("collected order = %v, want %v",
				segmentIDs(collected), segmentIDs(all))
		}
	}
	expectBases(t, collected, []int64{0, 100, 200, 300, 400})

	// A cursor on the final segment is still valid and yields an empty tail,
	// never restarting from the first page.
	status, data := fixture.do(t, http.MethodGet,
		fmt.Sprintf("/v1/streams/%s/segments?limit=2&after=%s", streamID, all[len(all)-1].ID), nil)
	body := listOK(t, status, data)
	if len(body.Items) != 0 {
		t.Fatalf("page after last segment = %v, want empty", segmentIDs(body.Items))
	}
}

func TestListSegmentsUnknownCursorRejected(t *testing.T) {
	fixture := newTestFixture(t)
	streamID, _ := fixture.createStreamWithSegments(t, "alpha", 4)

	status, data := fixture.do(t, http.MethodGet,
		fmt.Sprintf("/v1/streams/%s/segments?after=seg_unknown_cursor", streamID), nil)
	body := expectInvalidCursor(t, status, data)
	if body.Details["after"] != "seg_unknown_cursor" {
		t.Fatalf("error details = %v, want after detail", body.Details)
	}
}

func TestListSegmentsCrossStreamCursorRejected(t *testing.T) {
	fixture := newTestFixture(t)
	streamA, segmentsA := fixture.createStreamWithSegments(t, "alpha", 4)
	streamB, segmentsB := fixture.createStreamWithSegments(t, "beta", 2)

	// Cursor owned by stream B cannot drive pagination of stream A.
	status, data := fixture.do(t, http.MethodGet,
		fmt.Sprintf("/v1/streams/%s/segments?after=%s", streamA, segmentsB[0].ID), nil)
	expectInvalidCursor(t, status, data)

	// And the reverse direction must fail identically.
	status, data = fixture.do(t, http.MethodGet,
		fmt.Sprintf("/v1/streams/%s/segments?after=%s", streamB, segmentsA[0].ID), nil)
	expectInvalidCursor(t, status, data)
}

func TestListSegmentsCursorOutsideStateFilterRejected(t *testing.T) {
	fixture := newTestFixture(t)
	streamID, all := fixture.createStreamWithSegments(t, "alpha", 4)

	// The tail segment is open and therefore excluded by states=sealed.
	openSegment := all[len(all)-1]
	if openSegment.State != string(domain.SegmentStateOpen) {
		t.Fatalf("tail segment state = %s, want open", openSegment.State)
	}
	status, data := fixture.do(t, http.MethodGet,
		fmt.Sprintf("/v1/streams/%s/segments?states=sealed&limit=2&after=%s",
			streamID, openSegment.ID), nil)
	expectInvalidCursor(t, status, data)

	// Pagination strictly within the sealed filter visits every sealed
	// segment exactly once and never leaks the open segment.
	var collected []segmentView
	after := ""
	for {
		path := fmt.Sprintf("/v1/streams/%s/segments?states=sealed&limit=2", streamID)
		if after != "" {
			path += "&after=" + after
		}
		pageStatus, pageData := fixture.do(t, http.MethodGet, path, nil)
		body := listOK(t, pageStatus, pageData)
		if len(body.Items) == 0 {
			break
		}
		for _, item := range body.Items {
			if item.State != string(domain.SegmentStateSealed) {
				t.Fatalf("sealed filter returned state %s", item.State)
			}
		}
		collected = append(collected, body.Items...)
		after = body.Items[len(body.Items)-1].ID
	}
	if len(collected) != 4 {
		t.Fatalf("sealed total = %d, want 4", len(collected))
	}
	expectBases(t, collected, []int64{0, 100, 200, 300})
}

func TestListSegmentsWithoutCursorUnchanged(t *testing.T) {
	fixture := newTestFixture(t)
	streamID, all := fixture.createStreamWithSegments(t, "alpha", 4)

	// Default request (no limit, no cursor) still returns the full ordered set.
	status, data := fixture.do(t, http.MethodGet,
		"/v1/streams/"+streamID+"/segments", nil)
	body := listOK(t, status, data)
	if len(body.Items) != len(all) {
		t.Fatalf("default list size = %d, want %d", len(body.Items), len(all))
	}
	expectBases(t, body.Items, []int64{0, 100, 200, 300, 400})

	// The first page without a cursor starts at the first segment.
	status, data = fixture.do(t, http.MethodGet,
		fmt.Sprintf("/v1/streams/%s/segments?limit=2", streamID), nil)
	firstPage := listOK(t, status, data)
	if firstPage.Items[0].ID != all[0].ID {
		t.Fatalf("first page begins at %s, want %s",
			firstPage.Items[0].ID, all[0].ID)
	}

	// A valid mid-list cursor resumes immediately after it without skipping.
	status, data = fixture.do(t, http.MethodGet,
		fmt.Sprintf("/v1/streams/%s/segments?limit=2&after=%s", streamID, all[0].ID), nil)
	resumed := listOK(t, status, data)
	if len(resumed.Items) != 2 || resumed.Items[0].ID != all[1].ID {
		t.Fatalf("resumed page = %v, want %s, %s",
			segmentIDs(resumed.Items), all[1].ID, all[2].ID)
	}

	// Pre-existing validation and lookup behavior is untouched.
	status, data = fixture.do(t, http.MethodGet,
		"/v1/streams/"+streamID+"/segments?states=bogus", nil)
	if status != http.StatusBadRequest {
		t.Fatalf("unknown state status = %d, want 400", status)
	}
	status, _ = fixture.do(t, http.MethodGet,
		"/v1/streams/str_missing/segments", nil)
	if status != http.StatusNotFound {
		t.Fatalf("unknown stream status = %d, want 404", status)
	}
}
