package terminal

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Het-Jethva/Transferly/internal/session"
)

// offerProgress combines per-file progress with an aggregate, bounded view of
// one active Transfer Offer. It retains one counter per active stream, not file
// content, so its memory use is independent of transferred byte counts.
type offerProgress struct {
	app        *App
	fileCount  int
	totalBytes int64
	started    time.Time

	mu             sync.Mutex
	active         map[string]int64
	fileSizes      map[string]int64
	aggregateBytes int64
	completedFiles int
}

func newOfferProgress(app *App, fileCount int, totalBytes int64) *offerProgress {
	return &offerProgress{
		app:        app,
		fileCount:  fileCount,
		totalBytes: totalBytes,
		started:    time.Now(),
		active:     make(map[string]int64, 4),
		fileSizes:  make(map[string]int64, 4),
	}
}

func (p *offerProgress) begin(path string, size int64) (session.Progress, func()) {
	legacy := p.app.progress("Sending "+path, size)
	p.mu.Lock()
	p.active[path] = 0
	p.fileSizes[path] = size
	p.reportLocked(path)
	p.mu.Unlock()

	last := int64(0)
	observe := func(completed int64) {
		legacy(completed)
		p.mu.Lock()
		delta := completed - last
		if delta > 0 {
			p.aggregateBytes += delta
			last = completed
		}
		p.active[path] = completed
		if completed == 0 || completed == size || completed%(1024*1024) < delta {
			p.reportLocked(path)
		}
		p.mu.Unlock()
	}
	finished := func() {
		p.mu.Lock()
		delete(p.active, path)
		delete(p.fileSizes, path)
		p.mu.Unlock()
	}
	return observe, finished
}

func (p *offerProgress) complete(path string, succeeded bool) {
	p.mu.Lock()
	p.completedFiles++
	completed := p.completedFiles
	total := p.fileCount
	p.mu.Unlock()
	if succeeded {
		p.app.line("Item succeeded: %s (%d/%d files completed).", path, completed, total)
	} else {
		p.app.line("Item failed: %s (%d/%d files completed).", path, completed, total)
	}
}

func (p *offerProgress) reportLocked(current string) {
	paths := make([]string, 0, len(p.active))
	for path := range p.active {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	currentBytes := p.active[current]
	currentTotal := p.fileSizes[current]
	elapsed := time.Since(p.started)
	rate := float64(0)
	if elapsed > 0 {
		rate = float64(p.aggregateBytes) / elapsed.Seconds()
	}
	eta := "calculating"
	if rate > 0 && p.aggregateBytes < p.totalBytes {
		eta = (time.Duration(float64(p.totalBytes-p.aggregateBytes)/rate) * time.Second).Round(time.Second).String()
	} else if p.aggregateBytes >= p.totalBytes {
		eta = "0s"
	}
	p.app.line(
		"Active files: %d | Completed: %d/%d | Current: %s %d/%d bytes | Aggregate: %d/%d bytes | Rate: %s | ETA: %s | Streams: %s",
		len(p.active), p.completedFiles, p.fileCount, current, currentBytes, currentTotal,
		p.aggregateBytes, p.totalBytes, formatByteRate(rate), eta, strings.Join(paths, ", "),
	)
}

func formatByteRate(bytesPerSecond float64) string {
	const (
		kib = 1024
		mib = 1024 * kib
		gib = 1024 * mib
	)
	switch {
	case bytesPerSecond >= gib:
		return fmt.Sprintf("%.1f GiB/s", bytesPerSecond/gib)
	case bytesPerSecond >= mib:
		return fmt.Sprintf("%.1f MiB/s", bytesPerSecond/mib)
	case bytesPerSecond >= kib:
		return fmt.Sprintf("%.1f KiB/s", bytesPerSecond/kib)
	default:
		return fmt.Sprintf("%.0f B/s", bytesPerSecond)
	}
}
