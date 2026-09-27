package runtimecheck

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/application"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/domain"
)

type apiErrorEnvelope struct {
	Error struct {
		Code    string         `json:"code"`
		Message string         `json:"message"`
		Details map[string]any `json:"details,omitempty"`
	} `json:"error"`
}

// runSegmentPagination walks a stream with four segments page by page and
// proves that cursors are interpreted only inside the requested stream and
// filtered result: unknown, foreign-stream, and filtered-out cursors fail
// with a stable error rather than silently restarting at the first page.
func runSegmentPagination(
	ctx context.Context,
	assertions *counterAssertions,
) error {
	h, err := newHarness()
	if err != nil {
		return err
	}
	defer h.close()

	alphaID, alphaSegments, err := seedSegmentedStream(ctx, h, "field-lab", "alpha")
	if err != nil {
		return err
	}
	_, betaSegments, err := seedSegmentedStream(ctx, h, "field-lab", "beta")
	if err != nil {
		return err
	}

	// Baseline: the cursor-less list keeps its original behavior and returns
	// every segment once, ordered by base offset (identifier breaks ties).
	baseline, err := listSegmentPage(ctx, h, alphaID, "", "", 100)
	if err != nil {
		return err
	}
	if err := assertions.require(
		len(baseline) == 4,
		"cursor-less list returned %d segments, want 4",
		len(baseline),
	); err != nil {
		return err
	}
	baselineIDs := make([]string, 0, len(baseline))
	for index, segment := range baseline {
		if segment == nil {
			return fmt.Errorf("baseline page contains a nil segment")
		}
		baselineIDs = append(baselineIDs, segment.ID)
		if segment.StreamID != alphaID {
			return fmt.Errorf("segment %s belongs to %s, want %s", segment.ID, segment.StreamID, alphaID)
		}
		if index > 0 && baseline[index-1].Range.BaseOffset >= segment.Range.BaseOffset {
			return fmt.Errorf("baseline order is not ascending at index %d", index)
		}
	}
	if err := assertions.require(
		equalStrings(baselineIDs, alphaSegments),
		"baseline order %v does not match creation order %v",
		baselineIDs,
		alphaSegments,
	); err != nil {
		return err
	}

	// Normal page-by-page read with limit=2: pages join back into the full
	// ordered list exactly once, then the terminal page is an empty 200.
	page, err := listSegmentPage(ctx, h, alphaID, "", "", 2)
	if err != nil {
		return err
	}
	if err := assertions.require(
		equalStrings(segmentIDList(page), alphaSegments[:2]),
		"first page order is %v, want %v",
		segmentIDList(page),
		alphaSegments[:2],
	); err != nil {
		return err
	}

	page, err = listSegmentPage(ctx, h, alphaID, alphaSegments[1], "", 2)
	if err != nil {
		return err
	}
	if err := assertions.require(
		equalStrings(segmentIDList(page), alphaSegments[2:]),
		"second page order is %v, want %v",
		segmentIDList(page),
		alphaSegments[2:],
	); err != nil {
		return err
	}

	// Last page: cursor on the final segment yields a stable empty 200 and
	// never wraps around to the first page.
	tail, err := listSegmentPage(ctx, h, alphaID, alphaSegments[3], "", 2)
	if err != nil {
		return err
	}
	if err := assertions.require(
		len(tail) == 0,
		"page after final segment returned %d items, want 0",
		len(tail),
	); err != nil {
		return err
	}
	tail, err = listSegmentPage(ctx, h, alphaID, alphaSegments[2], "", 2)
	if err != nil {
		return err
	}
	if err := assertions.require(
		equalStrings(segmentIDList(tail), alphaSegments[3:]),
		"single-item last page is %v, want %v",
		segmentIDList(tail),
		alphaSegments[3:],
	); err != nil {
		return err
	}

	// Unknown cursor: stable 400 invalid_input instead of the first page.
	if err := expectCursorError(ctx, h, assertions, alphaID, "seg_000999", ""); err != nil {
		return err
	}

	// Cross-stream cursor: a real segment of beta must not page alpha.
	if err := expectCursorError(ctx, h, assertions, alphaID, betaSegments[1], ""); err != nil {
		return err
	}

	// Cursor excluded by the state filter: the first segment is sealed, so it
	// cannot anchor an open-only page; the sealed filter pages normally.
	if err := expectCursorError(ctx, h, assertions, alphaID, alphaSegments[0], "open"); err != nil {
		return err
	}
	openOnly, err := listSegmentPage(ctx, h, alphaID, "", "open", 100)
	if err != nil {
		return err
	}
	if err := assertions.require(
		equalStrings(segmentIDList(openOnly), alphaSegments[3:]),
		"open filter returned %v, want %v",
		segmentIDList(openOnly),
		alphaSegments[3:],
	); err != nil {
		return err
	}
	sealedPage, err := listSegmentPage(ctx, h, alphaID, "", "sealed", 2)
	if err != nil {
		return err
	}
	if err := assertions.require(
		equalStrings(segmentIDList(sealedPage), alphaSegments[:2]),
		"sealed first page is %v, want %v",
		segmentIDList(sealedPage),
		alphaSegments[:2],
	); err != nil {
		return err
	}
	sealedTail, err := listSegmentPage(ctx, h, alphaID, alphaSegments[1], "sealed", 2)
	if err != nil {
		return err
	}
	if err := assertions.require(
		equalStrings(segmentIDList(sealedTail), alphaSegments[2:3]),
		"sealed second page is %v, want %v",
		segmentIDList(sealedTail),
		alphaSegments[2:3],
	); err != nil {
		return err
	}

	// Unknown stream still reports not_found whether or not a cursor is sent.
	if err := expectStreamNotFound(ctx, h, assertions, "/v1/streams/str_000999/segments?limit=10"); err != nil {
		return err
	}
	if err := expectStreamNotFound(
		ctx, h, assertions,
		"/v1/streams/str_000999/segments?limit=10&after="+betaSegments[0],
	); err != nil {
		return err
	}

	// Simulated auto-pager with limit=1 must terminate and never repeat an
	// identifier; before the fix a bad cursor restart could loop or re-emit
	// the first segment.
	seen := map[string]bool{}
	autoOrder := make([]string, 0, len(baselineIDs))
	cursor := ""
	for request := 0; request < 20; request++ {
		page, err := listSegmentPage(ctx, h, alphaID, cursor, "", 1)
		if err != nil {
			return err
		}
		if len(page) == 0 {
			return assertions.require(
				equalStrings(autoOrder, baselineIDs),
				"auto-pager collected %v, want %v",
				autoOrder,
				baselineIDs,
			)
		}
		for _, segment := range page {
			if segment == nil {
				return fmt.Errorf("auto-pager page contains a nil segment")
			}
			if seen[segment.ID] {
				return fmt.Errorf("auto-pager repeated segment %s", segment.ID)
			}
			seen[segment.ID] = true
			autoOrder = append(autoOrder, segment.ID)
			cursor = segment.ID
		}
	}
	return fmt.Errorf("auto-pager did not terminate after 20 requests: %v", autoOrder)
}

// seedSegmentedStream creates a stream at a 64-byte threshold and appends
// three 64-byte batches, so the result holds four segments ordered at base
// offsets 0, 64, 128, 192 (the first three sealed, the last one open).
func seedSegmentedStream(
	ctx context.Context,
	h *harness,
	namespace string,
	name string,
) (string, []string, error) {
	var created application.CreateStreamResult
	if err := h.call(ctx, http.MethodPost, "/v1/streams", map[string]any{
		"namespace":       namespace,
		"name":            name,
		"threshold_bytes": 64,
	}, &created); err != nil {
		return "", nil, err
	}
	streamID := created.Stream.ID
	ids := []string{created.Segment.ID}
	revision := uint64(1)
	for range 3 {
		var appended application.AppendRecordsResult
		if err := h.call(
			ctx,
			http.MethodPost,
			"/v1/streams/"+streamID+"/records",
			map[string]any{
				"expected_revision": revision,
				"record_count":      1,
				"payload_bytes":     64,
			},
			&appended,
		); err != nil {
			return "", nil, err
		}
		if !appended.RolledOver {
			return "", nil, fmt.Errorf("append at revision %d did not roll over", revision)
		}
		revision++
		ids = append(ids, appended.Segment.ID)
	}
	return streamID, ids, nil
}

func listSegmentPage(
	ctx context.Context,
	h *harness,
	streamID string,
	after string,
	state string,
	limit int,
) ([]*domain.Segment, error) {
	query := []string{fmt.Sprintf("limit=%d", limit)}
	if after != "" {
		query = append(query, "after="+after)
	}
	if state != "" {
		query = append(query, "states="+state)
	}
	path := fmt.Sprintf(
		"/v1/streams/%s/segments?%s",
		streamID,
		strings.Join(query, "&"),
	)
	var response segmentList
	if err := h.callExpect(ctx, http.MethodGet, path, nil, http.StatusOK, &response); err != nil {
		return nil, err
	}
	return response.Items, nil
}

// expectCursorError asserts that the cursor is rejected with the stable
// invalid_input error instead of silently paging from the start.
func expectCursorError(
	ctx context.Context,
	h *harness,
	assertions *counterAssertions,
	streamID string,
	after string,
	state string,
) error {
	query := "limit=2&after=" + after
	if state != "" {
		query += "&states=" + state
	}
	path := "/v1/streams/" + streamID + "/segments?" + query
	var envelope apiErrorEnvelope
	if err := h.callExpect(
		ctx, http.MethodGet, path, nil, http.StatusBadRequest, &envelope,
	); err != nil {
		return err
	}
	return assertions.require(
		envelope.Error.Code == string(domain.CodeInvalidInput),
		"cursor %q on %s returned code %q, want %q",
		after,
		path,
		envelope.Error.Code,
		domain.CodeInvalidInput,
	)
}

func expectStreamNotFound(
	ctx context.Context,
	h *harness,
	assertions *counterAssertions,
	path string,
) error {
	var envelope apiErrorEnvelope
	if err := h.callExpect(
		ctx, http.MethodGet, path, nil, http.StatusNotFound, &envelope,
	); err != nil {
		return err
	}
	return assertions.require(
		envelope.Error.Code == string(domain.CodeNotFound),
		"%s returned code %q, want %q",
		path,
		envelope.Error.Code,
		domain.CodeNotFound,
	)
}

func segmentIDList(segments []*domain.Segment) []string {
	ids := make([]string, 0, len(segments))
	for _, segment := range segments {
		if segment != nil {
			ids = append(ids, segment.ID)
		}
	}
	return ids
}

func equalStrings(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
