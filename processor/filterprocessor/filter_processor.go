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
	"fmt"
	"strconv"

	"go.uber.org/zap"

	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/internal/processor/filtermetric"
	"go.opentelemetry.io/collector/processor/processorhelper"
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
		if rm.IsNil() {
			continue
		}

		keepMetricsForResource, err := fmp.shouldKeepMetricsForResource(rm.Resource())
		if err != nil {
			fmp.logger.Error("shouldKeepMetricsForResource failed", zap.Error(err))
			// don't `continue`, keep the metric if there's an error
		}
		if !keepMetricsForResource {
			continue
		}

		ilms := rm.InstrumentationLibraryMetrics()
		for j := 0; j < ilms.Len(); j++ {
			ilm := ilms.At(j)
			if ilm.IsNil() {
				continue
			}
			ms := ilm.Metrics()
			for k := 0; k < ms.Len(); k++ {
				metric := ms.At(k)
				if metric.IsNil() {
					continue
				}
				keep, err := fmp.shouldKeepMetric(metric)
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

func (fmp *filterMetricProcessor) shouldKeepMetricsForResource(resource pdata.Resource) (bool, error) {
	// default to keep if tries to filter metrics with null resource
	if resource.IsNil() {
		return true, fmt.Errorf("Metrics with null resource")
	}
	resourceAttributes := resource.Attributes()

	if fmp.include != nil {
		for key, filterList := range fmp.cfg.Metrics.Include.ResourceAttributes {
			attributeValue, ok := resourceAttributes.Get(key)
			if ok {
				attributeMatched, err := attributeMatch(filterList, attributeValue)
				// default to keep if there's an error
				if err != nil {
					return true, err
				}
				if !attributeMatched {
					return false, nil
				}
			}
		}
	}

	if fmp.exclude != nil {
		for key, filterList := range fmp.cfg.Metrics.Exclude.ResourceAttributes {
			attributeValue, ok := resourceAttributes.Get(key)
			if ok {
				attributeMatched, err := attributeMatch(filterList, attributeValue)
				// default to keep if there's an error
				if err != nil {
					return true, err
				}
				if attributeMatched {
					return false, nil
				}
			}
		}
	}

	return true, nil
}

func attributeMatch(filterList []string, attributeValue pdata.AttributeValue) (bool, error) {
	attribute := ""
	switch attributeValue.Type() {
	case pdata.AttributeValueSTRING:
		attribute = attributeValue.StringVal()
	case pdata.AttributeValueBOOL:
		attribute = strconv.FormatBool(attributeValue.BoolVal())
	case pdata.AttributeValueINT:
		attribute = strconv.FormatInt(attributeValue.IntVal(), 10)
	default:
		return true, fmt.Errorf("Filter processor cannot convert attribute type to string")
	}

	for _, item := range filterList {
		if item == attribute {
			return true, nil
		}
	}
	return false, nil
}
