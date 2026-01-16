package cmd

import (
	"os"
	"time"

	"github.com/schollz/progressbar/v3"
)

// progress wraps schollz/progressbar with an opt-out flag (reportEvery == 0).
type progress struct {
	bar *progressbar.ProgressBar
}

func newProgress(total, reportEvery int) *progress {
	if reportEvery == 0 {
		return &progress{bar: nil}
	}

	opts := []progressbar.Option{
		progressbar.OptionSetWriter(os.Stderr),
		progressbar.OptionThrottle(250 * time.Millisecond),
		progressbar.OptionClearOnFinish(),
	}

	var bar *progressbar.ProgressBar
	if total > 0 {
		opts = append(opts,
			progressbar.OptionSetWidth(30),
			progressbar.OptionShowCount(),
			progressbar.OptionShowIts(),
			progressbar.OptionSetPredictTime(true),
		)
		bar = progressbar.NewOptions(total, opts...)
	} else {
		opts = append(opts,
			progressbar.OptionSpinnerType(14),
			progressbar.OptionShowCount(),
			progressbar.OptionShowIts(),
		)
		bar = progressbar.NewOptions(-1, opts...)
	}

	return &progress{bar: bar}
}

func (p *progress) increment() {
	if p.bar == nil {
		return
	}
	_ = p.bar.Add(1)
}

func (p *progress) finish() {
	if p.bar == nil {
		return
	}
	_ = p.bar.Finish()
}
