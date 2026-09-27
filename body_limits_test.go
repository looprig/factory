package factory

import (
	"strings"
	"testing"

	"github.com/looprig/factory/internal/httpapi"
)

func TestInboundBodyLimitValidation(t *testing.T) {
	for _, size := range []int{0, 64<<10 - 1, 16<<20 + 1} {
		limits := DefaultClientLinkLimits()
		limits.MaxMessageBytes = size
		if err := limits.Validate(); err == nil || !strings.Contains(err.Error(), "MaxMessageBytes") {
			t.Errorf("ClientLink max %d: %v", size, err)
		}
		if _, err := New(append(RequiredOptions(), WithClientLinkLimits(limits))...); err == nil || !strings.Contains(err.Error(), "MaxMessageBytes") {
			t.Errorf("New with ClientLink max %d: %v", size, err)
		}
	}
	for _, size := range []int64{-1, 1<<20 - 1, 16<<20 + 1} {
		limits := httpapi.DefaultRouteLimits()
		limits.MaxCommandBytes = size
		if err := limits.Validate(); err == nil || !strings.Contains(err.Error(), "MaxCommandBytes") {
			t.Errorf("REST command max %d: %v", size, err)
		}
		if _, err := New(append(RequiredOptions(), WithRouteLimits(limits))...); err == nil || !strings.Contains(err.Error(), "MaxCommandBytes") {
			t.Errorf("New with REST command max %d: %v", size, err)
		}
	}
}
