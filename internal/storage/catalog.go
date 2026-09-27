package storage

import (
	"sort"
	"strings"
	"time"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/domain"
)

func domainTimeZero() time.Time {
	return time.Time{}
}

type SegmentFilter struct {
	StreamID string
	States   map[domain.SegmentState]bool
	AfterID  string
	Limit    int
}

func ListSegments(state *domain.State, filter SegmentFilter) ([]*domain.Segment, error) {
	if state == nil {
		return []*domain.Segment{}, nil
	}
	result := make([]*domain.Segment, 0)
	for _, segment := range state.Segments {
		if segment == nil {
			continue
		}
		if filter.StreamID != "" && segment.StreamID != filter.StreamID {
			continue
		}
		if len(filter.States) > 0 && !filter.States[segment.State] {
			continue
		}
		result = append(result, domain.CloneState(&domain.State{
			SchemaVersion: domain.SnapshotSchemaVersion,
			Streams:       map[string]*domain.Stream{},
			Segments:      map[string]*domain.Segment{segment.ID: segment},
			Plans:         map[string]*domain.CompactionPlan{},
		}).Segments[segment.ID])
	}
	sort.Slice(result, func(left, right int) bool {
		if result[left].Range.BaseOffset == result[right].Range.BaseOffset {
			return result[left].ID < result[right].ID
		}
		return result[left].Range.BaseOffset < result[right].Range.BaseOffset
	})
	if filter.AfterID != "" {
		start := -1
		for index, segment := range result {
			if segment.ID == filter.AfterID {
				start = index + 1
				break
			}
		}
		if start < 0 {
			return nil, invalidCursorError(state, filter)
		}
		result = result[start:]
	}
	if filter.Limit > 0 && len(result) > filter.Limit {
		result = result[:filter.Limit]
	}
	return result, nil
}

// invalidCursorError builds a stable error for an after cursor that cannot be
// interpreted within the filtered, ordered segment set. A cursor may be
// unknown, belong to another stream, or belong to a segment that no longer
// matches the active state filter.
func invalidCursorError(state *domain.State, filter SegmentFilter) error {
	cursor := state.Segments[filter.AfterID]
	switch {
	case cursor == nil:
		return domain.NewError(domain.CodeInvalidCursor, "pagination cursor does not identify a segment").
			WithDetail("after", filter.AfterID)
	case filter.StreamID != "" && cursor.StreamID != filter.StreamID:
		return domain.NewError(domain.CodeInvalidCursor, "pagination cursor belongs to another stream").
			WithDetail("after", filter.AfterID).
			WithDetail("stream_id", cursor.StreamID)
	default:
		return domain.NewError(
			domain.CodeInvalidCursor,
			"pagination cursor is not part of the filtered segment set",
		).WithDetail("after", filter.AfterID)
	}
}

func FindStreamByName(state *domain.State, namespace, name string) *domain.Stream {
	if state == nil {
		return nil
	}
	namespace = strings.TrimSpace(namespace)
	name = strings.TrimSpace(name)
	for _, stream := range state.Streams {
		if stream != nil && stream.Namespace == namespace && stream.Name == name {
			return cloneStreamForRead(stream)
		}
	}
	return nil
}

func FindStream(state *domain.State, streamID string) *domain.Stream {
	if state == nil {
		return nil
	}
	return cloneStreamForRead(state.Streams[streamID])
}

func FindSegment(state *domain.State, segmentID string) *domain.Segment {
	if state == nil || state.Segments[segmentID] == nil {
		return nil
	}
	copyState := &domain.State{
		SchemaVersion: domain.SnapshotSchemaVersion,
		Streams:       map[string]*domain.Stream{},
		Segments: map[string]*domain.Segment{
			segmentID: state.Segments[segmentID],
		},
		Plans: map[string]*domain.CompactionPlan{},
	}
	return domain.CloneState(copyState).Segments[segmentID]
}

func FindPlan(state *domain.State, planID string) *domain.CompactionPlan {
	if state == nil || state.Plans[planID] == nil {
		return nil
	}
	copyState := &domain.State{
		SchemaVersion: domain.SnapshotSchemaVersion,
		Streams:       map[string]*domain.Stream{},
		Segments:      map[string]*domain.Segment{},
		Plans: map[string]*domain.CompactionPlan{
			planID: state.Plans[planID],
		},
	}
	return domain.CloneState(copyState).Plans[planID]
}

func cloneStreamForRead(source *domain.Stream) *domain.Stream {
	if source == nil {
		return nil
	}
	copyState := domain.CloneState(&domain.State{
		SchemaVersion: domain.SnapshotSchemaVersion,
		Streams: map[string]*domain.Stream{
			source.ID: source,
		},
		Segments: map[string]*domain.Segment{},
		Plans:    map[string]*domain.CompactionPlan{},
	})
	return copyState.Streams[source.ID]
}
