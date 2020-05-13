// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storepb

import (
	"strings"
	"unsafe"

	"github.com/gogo/protobuf/types"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

var PartialResponseStrategyValues = func() []string {
	var s []string
	for k := range PartialResponseStrategy_value {
		s = append(s, k)
	}
	return s
}()

func NewWarnSeriesResponse(err error) *SeriesResponse {
	return &SeriesResponse{
		Result: &SeriesResponse_Warning{
			Warning: err.Error(),
		},
	}
}

func NewSeriesResponse(series *Series) *SeriesResponse {
	return &SeriesResponse{
		Result: &SeriesResponse_Series{
			Series: series,
		},
	}
}

func NewHintsSeriesResponse(hints *types.Any) *SeriesResponse {
	return &SeriesResponse{
		Result: &SeriesResponse_Hints{
			Hints: hints,
		},
	}
}

// CompareLabels compares two sets of labels.
func CompareLabels(a, b []Label) int {
	l := len(a)
	if len(b) < l {
		l = len(b)
	}
	for i := 0; i < l; i++ {
		if d := strings.Compare(a[i].Name, b[i].Name); d != 0 {
			return d
		}
		if d := strings.Compare(a[i].Value, b[i].Value); d != 0 {
			return d
		}
	}
	// If all labels so far were in common, the set with fewer labels comes first.
	return len(a) - len(b)
}

type emptySeriesSet struct{}

func (emptySeriesSet) Next() bool                 { return false }
func (emptySeriesSet) At() ([]Label, []AggrChunk) { return nil, nil }
func (emptySeriesSet) Err() error                 { return nil }

// EmptySeriesSet returns a new series set that contains no series.
func EmptySeriesSet() SeriesSet {
	return emptySeriesSet{}
}

// MergeSeriesSets takes all series sets and returns as a union single series set.
// It assumes series are sorted by labels within single SeriesSet, similar to remote read guarantees.
// However, they can be partial: in such case, if the single SeriesSet returns the same series within many iterations,
// MergeSeriesSets will merge those into one.
//
// It also assumes in a "best effort" way that chunks are sorted by min time. It's done as an optimization only, so if input
// series' chunks are NOT sorted, the only consequence is that the duplicates might be not correctly removed. This is double checked
// which on just-before PromQL level as well, so the only consequence is increased network bandwidth.
// If all chunks were sorted, MergeSeriesSet ALSO returns sorted chunks by min time.
//
// Chunks within the same series can also overlap (within all SeriesSet
// as well as single SeriesSet alone). If the chunk ranges overlap, the *exact* chunk duplicates will be removed
// (except one), and any other overlaps will be appended into on chunks slice.
func MergeSeriesSets(all ...SeriesSet) SeriesSet {
	switch len(all) {
	case 0:
		return emptySeriesSet{}
	case 1:
		return all[0]
	}
	h := len(all) / 2

	return newMergedSeriesSet(
		MergeSeriesSets(all[:h]...),
		MergeSeriesSets(all[h:]...),
	)
}

// SeriesSet is a set of series and their corresponding chunks.
// The set is sorted by the label sets. Chunks may be overlapping or expected of order.
type SeriesSet interface {
	Next() bool
	At() ([]Label, []AggrChunk)
	Err() error
}

// mergedSeriesSet takes two series sets as a single series set.
type mergedSeriesSet struct {
	a, b SeriesSet
	// peek nil means not set iterator exhausted or not started.
	apeek, bpeek *Series
	aok, bok     bool

	// Current.
	curr *Series
}

func (s *mergedSeriesSet) adone() bool { return s.apeek == nil }
func (s *mergedSeriesSet) bdone() bool { return s.bpeek == nil }

func newMergedSeriesSet(a, b SeriesSet) *mergedSeriesSet {
	s := &mergedSeriesSet{a: a, b: b}
	s.apeek, s.aok = peekSameLset(s.a, s.a.Next())
	s.bpeek, s.bok = peekSameLset(s.b, s.b.Next())
	return s
}

func peekSameLset(ss SeriesSet, sok bool) (peek *Series, _ bool) {
	if !sok {
		return nil, false
	}

	peek = &Series{}
	peek.Labels, peek.Chunks = ss.At()
	for ss.Next() {
		lset, chks := ss.At()
		if CompareLabels(lset, peek.Labels) != 0 {
			return peek, true
		}

		// TODO: Slice reuse is not generally safe with nested merge iterators?
		peek.Chunks = append(peek.Chunks, chks...)
	}
	return peek, false
}

func (s *mergedSeriesSet) At() ([]Label, []AggrChunk) {
	if s.curr == nil {
		return nil, nil
	}
	return s.curr.Labels, s.curr.Chunks
}

func (s *mergedSeriesSet) Err() error {
	if s.a.Err() != nil {
		return s.a.Err()
	}
	return s.b.Err()
}

func (s *mergedSeriesSet) comparePeeks() int {
	if s.adone() {
		return 1
	}
	if s.bdone() {
		return -1
	}
	return CompareLabels(s.apeek.Labels, s.bpeek.Labels)
}

func (s *mergedSeriesSet) Next() bool {
	if (s.adone() && s.bdone()) || s.Err() != nil {
		return false
	}

	d := s.comparePeeks()
	if d > 0 {
		s.curr = s.bpeek
		s.bpeek, s.bok = peekSameLset(s.b, s.bok)
		return true
	}
	if d < 0 {
		s.curr = s.apeek
		s.apeek, s.aok = peekSameLset(s.a, s.aok)
		return true
	}
	// Both a and b contains the same series. Go through all chunks, remove duplicates and concatenate chunks from both
	// series sets. We best effortly assume chunks are sorted by min time. If not, we will not detect all deduplicate which will
	// be account on select layer anyway. We do it still for early optimization.

	s.curr = &Series{
		Labels: s.apeek.Labels,
		// Slice reuse is not generally safe with nested merge iterators. We are on the safe side creating a new slice.
		Chunks: make([]AggrChunk, 0, len(s.apeek.Chunks)+len(s.bpeek.Chunks)),
	}

	b := 0
Outer:
	for a := range s.apeek.Chunks {
		for {
			if b >= len(s.bpeek.Chunks) {
				// No more b chunks.
				s.curr.Chunks = append(s.curr.Chunks, s.apeek.Chunks[a:]...)
				break Outer
			}

			if s.apeek.Chunks[a].MinTime < s.bpeek.Chunks[b].MinTime {
				s.curr.Chunks = append(s.curr.Chunks, s.apeek.Chunks[a])
				break
			}

			if s.apeek.Chunks[a].MinTime > s.bpeek.Chunks[b].MinTime {
				s.curr.Chunks = append(s.curr.Chunks, s.bpeek.Chunks[b])
				b++
				continue
			}

			if strings.Compare(s.apeek.Chunks[a].String(), s.bpeek.Chunks[b].String()) == 0 {
				// Exact duplicated chunks, discard one from b.
				b++
				continue
			}

			// Same min Time, but not duplicate, so it does not matter. Take b (since lower for loop).
			s.curr.Chunks = append(s.curr.Chunks, s.bpeek.Chunks[b])
			b++
		}
	}

	if b < len(s.bpeek.Chunks) {
		s.curr.Chunks = append(s.curr.Chunks, s.bpeek.Chunks[b:]...)
	}

	s.apeek, s.aok = peekSameLset(s.a, s.aok)
	s.bpeek, s.bok = peekSameLset(s.b, s.bok)
	return true
}

// LabelsToPromLabels converts Thanos proto labels to Prometheus labels in type safe manner.
func LabelsToPromLabels(lset []Label) labels.Labels {
	ret := make(labels.Labels, len(lset))
	for i, l := range lset {
		ret[i] = labels.Label{Name: l.Name, Value: l.Value}
	}
	return ret
}

// LabelsToPromLabelsUnsafe converts Thanos proto labels to Prometheus labels in type unsafe manner.
// It reuses the same memory. Caller should abort using passed []Labels.
//
// NOTE: This depends on order of struct fields etc, so use with extreme care.
func LabelsToPromLabelsUnsafe(lset []Label) labels.Labels {
	return *(*[]labels.Label)(unsafe.Pointer(&lset))
}

// PromLabelsToLabels converts Prometheus labels to Thanos proto labels in type safe manner.
func PromLabelsToLabels(lset labels.Labels) []Label {
	ret := make([]Label, len(lset))
	for i, l := range lset {
		ret[i] = Label{Name: l.Name, Value: l.Value}
	}
	return ret
}

// PromLabelsToLabelsUnsafe converts Prometheus labels to Thanos proto labels in type unsafe manner.
// It reuses the same memory. Caller should abort using passed labels.Labels.
//
// // NOTE: This depends on order of struct fields etc, so use with extreme care.
func PromLabelsToLabelsUnsafe(lset labels.Labels) []Label {
	return *(*[]Label)(unsafe.Pointer(&lset))
}

// PrompbLabelsToLabels converts Prometheus labels to Thanos proto labels in type safe manner.
func PrompbLabelsToLabels(lset []prompb.Label) []Label {
	ret := make([]Label, len(lset))
	for i, l := range lset {
		ret[i] = Label{Name: l.Name, Value: l.Value}
	}
	return ret
}

// PrompbLabelsToLabelsUnsafe converts Prometheus proto labels to Thanos proto labels in type unsafe manner.
// It reuses the same memory. Caller should abort using passed labels.Labels.
//
// // NOTE: This depends on order of struct fields etc, so use with extreme care.
func PrompbLabelsToLabelsUnsafe(lset []prompb.Label) []Label {
	return *(*[]Label)(unsafe.Pointer(&lset))
}

func LabelsToString(lset []Label) string {
	var s []string
	for _, l := range lset {
		s = append(s, l.String())
	}
	return "[" + strings.Join(s, ",") + "]"
}

func LabelSetsToString(lsets []LabelSet) string {
	s := []string{}
	for _, ls := range lsets {
		s = append(s, LabelsToString(ls.Labels))
	}
	return strings.Join(s, "")
}
