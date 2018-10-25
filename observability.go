package nds

import (
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
)

var (
	// Measures
	mCacheHit  = stats.Int64("cache_hit", "The number of cache hits", stats.UnitDimensionless)
	mCacheMiss = stats.Int64("cache_miss", "The number of cache misses", stats.UnitDimensionless)

	// Views
	AllViews = []*view.View{
		{
			Name:        "nds/cache_hit",
			Description: "The number of cache hits",
			Measure:     mCacheHit,
			Aggregation: view.Sum(),
		},
		{
			Name:        "nds/cache_miss",
			Description: "The number of cache misses",
			Measure:     mCacheMiss,
			Aggregation: view.Sum(),
		},
	}
)
