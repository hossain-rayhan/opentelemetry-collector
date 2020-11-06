// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package filterprocessor

import (
	"context"

	"go.uber.org/zap"

	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/internal/processor/filtermetric"
	"go.opentelemetry.io/collector/processor/processorhelper"
	tracetranslator "go.opentelemetry.io/collector/translator/trace"
)

type filterMetricProcessor struct {
	cfg     *Config
	include filtermetric.Matcher
	exclude filtermetric.Matcher
	logger  *zap.Logger
}

func newFilterMetricProcessor(logger *zap.Logger, cfg *Config) (*filterMetricProcessor, error) {
	inc, err := createMatcher(cfg.Metrics.Include)
	if err != nil {
		return nil, err
	}

	exc, err := createMatcher(cfg.Metrics.Exclude)
	if err != nil {
		return nil, err
	}

	includeMatchType := ""
	var includeExpressions []string
	var includeMetricNames []string
	var includeResourceAttributes map[string][]string
	if cfg.Metrics.Include != nil {
		includeMatchType = string(cfg.Metrics.Include.MatchType)
		includeExpressions = cfg.Metrics.Include.Expressions
		includeMetricNames = cfg.Metrics.Include.MetricNames
		includeResourceAttributes = cfg.Metrics.Include.ResourceAttributes
	}

	excludeMatchType := ""
	var excludeExpressions []string
	var excludeMetricNames []string
	var excludeResourceAttributes map[string][]string
	if cfg.Metrics.Exclude != nil {
		excludeMatchType = string(cfg.Metrics.Exclude.MatchType)
		excludeExpressions = cfg.Metrics.Exclude.Expressions
		excludeMetricNames = cfg.Metrics.Exclude.MetricNames
		excludeResourceAttributes = cfg.Metrics.Exclude.ResourceAttributes
	}

	logger.Info(
		"Metric filter configured",
		zap.String("include match_type", includeMatchType),
		zap.Strings("include expressions", includeExpressions),
		zap.Strings("include metric names", includeMetricNames),
		zap.Any("include metrics with resource attributes", includeResourceAttributes),
		zap.String("exclude match_type", excludeMatchType),
		zap.Strings("exclude expressions", excludeExpressions),
		zap.Strings("exclude metric names", excludeMetricNames),
		zap.Any("exclude metrics with resource attributes", excludeResourceAttributes),
	)

	return &filterMetricProcessor{
		cfg:     cfg,
		include: inc,
		exclude: exc,
		logger:  logger,
	}, nil
}

func createMatcher(mp *filtermetric.MatchProperties) (filtermetric.Matcher, error) {
	// Nothing specified in configuration
	if mp == nil {
		return nil, nil
	}
	return filtermetric.NewMatcher(mp)
}

// ProcessMetrics filters the given metrics based off the filterMetricProcessor's filters.
func (fmp *filterMetricProcessor) ProcessMetrics(_ context.Context, pdm pdata.Metrics) (pdata.Metrics, error) {
	rms := pdm.ResourceMetrics()
	idx := newMetricIndex()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)

		keepMetricsForResource := fmp.shouldKeepMetricsForResource(rm.Resource())
		if !keepMetricsForResource {
			continue
		}

		ilms := rm.InstrumentationLibraryMetrics()
		for j := 0; j < ilms.Len(); j++ {
			ms := ilms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				keep, err := fmp.shouldKeepMetric(ms.At(k))
				if err != nil {
					fmp.logger.Error("shouldKeepMetric failed", zap.Error(err))
					// don't `continue`, keep the metric if there's an error
				}
				if keep {
					idx.add(i, j, k)
				}
			}
		}
	}
	if idx.isEmpty() {
		return pdm, processorhelper.ErrSkipProcessingData
	}
	return idx.extract(pdm), nil
}

func (fmp *filterMetricProcessor) shouldKeepMetric(metric pdata.Metric) (bool, error) {
	if fmp.include != nil {
		matches, err := fmp.include.MatchMetric(metric)
		if err != nil {
			// default to keep if there's an error
			return true, err
		}
		if !matches {
			return false, nil
		}
	}

	if fmp.exclude != nil {
		matches, err := fmp.exclude.MatchMetric(metric)
		if err != nil {
			return true, err
		}
		if matches {
			return false, nil
		}
	}

	return true, nil
}

func (fmp *filterMetricProcessor) shouldKeepMetricsForResource(resource pdata.Resource) bool {
	resourceAttributes := resource.Attributes()

	if fmp.include != nil {
		for key, filterList := range fmp.cfg.Metrics.Include.ResourceAttributes {
			attributeValue, ok := resourceAttributes.Get(key)
			if ok {
				attributeMatched := attributeMatch(filterList, attributeValue)
				if !attributeMatched {
					return false
				}
			}
		}
	}

	if fmp.exclude != nil {
		for key, filterList := range fmp.cfg.Metrics.Exclude.ResourceAttributes {
			attributeValue, ok := resourceAttributes.Get(key)
			if ok {
				attributeMatched := attributeMatch(filterList, attributeValue)
				if attributeMatched {
					return false
				}
			}
		}
	}

	return true
}

func attributeMatch(filterList []string, attributeValue pdata.AttributeValue) bool {
	attribute := tracetranslator.AttributeValueToString(attributeValue, false)

	for _, item := range filterList {
		if item == attribute {
			return true
		}
	}
	return false
}
