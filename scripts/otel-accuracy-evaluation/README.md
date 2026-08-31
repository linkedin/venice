# OTEL Histogram Accuracy Visualization

This directory contains a self-contained visualization for exploring OpenTelemetry base2 exponential histogram accuracy
across latency ranges, `max.scale` values, and `max.buckets` values.

## Generate the HTML

From the repository root:

```bash
scripts/otel-accuracy-evaluation/otel_histogram_accuracy_range_3d_plot.py \
  --min-ms 0.05 \
  --max-ms 150 \
  --latency-ms 50 \
  --scales 3,4,5,6 \
  --buckets 250,300,350,370,450,500,600,650,700,740,800,900,1000 \
  --output scripts/otel-accuracy-evaluation/otel_histogram_accuracy_range_3d_plot.html
```

## Open the visualization

```bash
open scripts/otel-accuracy-evaluation/otel_histogram_accuracy_range_3d_plot.html
```

## What the controls mean

- `Smallest positive latency`: lower bound of the positive latency range. Exact `0 ms` is handled separately by OTEL
  zero count.
- `Largest positive latency`: upper bound of the latency range the histogram needs to cover.
- `Latency to evaluate`: latency value used to compute the plotted error height.
- `Min/Max configured max.scale`: scale range to compare.
- `max.buckets`: bucket caps compared on the X axis.

The plot height shows approximate bucket-level error in milliseconds. Green points preserve the configured scale; red
points indicate the effective scale was downshifted because the selected bucket cap was too small for the selected
range.

Plotly is vendored under `vendor/`, so the generated HTML can run without a CDN.
