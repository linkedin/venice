#!/usr/bin/env python3
"""
Generate an interactive range-aware 3D HTML plot for OpenTelemetry base2 exponential histogram accuracy.

In the generated HTML, you can select:
  - smallest positive latency in the observed range
  - largest positive latency in the observed range
  - latency value to evaluate

The plot then shows:
  X: max.buckets
  Y: configured max.scale
  Z: approximate bucket-level error for the selected latency

The plotted height accounts for effective scale downshifting when the selected
latency range does not fit inside max.buckets at the configured max.scale.

No Python plotting dependencies are required. The generated HTML uses a locally
vendored Plotly bundle from vendor/plotly-2.35.2.min.js.
"""

import argparse
import html
import json
from pathlib import Path
from string import Template
from typing import List


DEFAULT_SCALES = [3, 4, 5, 6]
DEFAULT_BUCKETS = [250, 300, 350, 370, 450, 500, 600, 650, 700, 740, 800, 900, 1000]
MIN_SUPPORTED_LATENCY_MS = 0.01
MAX_SUPPORTED_LATENCY_MS = 250.0
PLOTLY_VENDOR_PATH = "vendor/plotly-2.35.2.min.js"


def parse_csv_numbers(value: str, number_type=float) -> List:
    return [number_type(part.strip()) for part in value.split(",") if part.strip()]


def build_html(initial_min_ms: float, initial_max_ms: float, initial_latency_ms: float, scales: List[int], buckets: List[int]) -> str:
    title = "OTel Histogram Accuracy by Latency Range"
    payload = {
        "initialMinMs": initial_min_ms,
        "initialMaxMs": initial_max_ms,
        "initialLatencyMs": initial_latency_ms,
        "minSupportedLatencyMs": MIN_SUPPORTED_LATENCY_MS,
        "maxSupportedLatencyMs": MAX_SUPPORTED_LATENCY_MS,
        "scales": scales,
        "initialMinScale": min(scales),
        "initialMaxScale": max(scales),
        "buckets": buckets,
    }

    template = Template(
        r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>$title</title>
  <script src="$plotly_vendor_path"></script>
  <style>
    body { font-family: Arial, sans-serif; margin: 24px; color: #222; }
    #plot { width: 100%; height: 760px; }
    .controls {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 14px;
      max-width: 1080px;
      padding: 16px;
      border: 1px solid #ddd;
      border-radius: 8px;
      background: #fafafa;
    }
    .control { display: flex; flex-direction: column; gap: 6px; }
    label { font-weight: 600; }
    input { padding: 7px; font-size: 14px; }
    button { padding: 8px 10px; cursor: pointer; }
    .note { max-width: 1080px; line-height: 1.45; }
    .summary { margin: 16px 0; font-weight: 600; }
    .error { color: #b00020; font-weight: 700; }
    table { border-collapse: collapse; margin-top: 24px; width: 100%; max-width: 1180px; }
    th, td { border: 1px solid #ddd; padding: 6px 10px; text-align: right; }
    th { background: #f5f5f5; }
    td:first-child, th:first-child { text-align: left; }
    .downscaled { color: #d62728; font-weight: bold; }
    .preserved { color: #2ca02c; font-weight: bold; }
  </style>
</head>
<body>
  <h1>$title</h1>

  <p class="note">
    Select the positive latency range that the histogram needs to cover, then choose the latency value to evaluate.
    The 3D plot uses error level as height. Exact <code>0 ms</code> values are tracked separately by the OTel
    exponential histogram zero count, so the minimum below should be the smallest positive latency you expect.
    The interactive range is bounded to <strong>0.01 ms – 250 ms</strong>; defaults remain <strong>0.05 ms – 150 ms</strong>.
  </p>

  <div class="controls">
    <div class="control">
      <label for="minMs">Smallest positive latency, ms</label>
      <input id="minMs" type="number" min="0.01" max="250" step="0.001">
    </div>
    <div class="control">
      <label for="maxMs">Largest positive latency, ms</label>
      <input id="maxMs" type="number" min="0.01" max="250" step="0.001">
    </div>
    <div class="control">
      <label for="latencyMs">Latency to evaluate, ms</label>
      <input id="latencyMs" type="number" min="0.01" max="250" step="0.001">
      <input id="latencySlider" type="range" min="0.01" max="250" step="0.001">
    </div>
    <div class="control">
      <label for="minScale">Min configured max.scale</label>
      <input id="minScale" type="number" min="3" max="6" step="1">
    </div>
    <div class="control">
      <label for="maxScale">Max configured max.scale</label>
      <input id="maxScale" type="number" min="3" max="6" step="1">
    </div>
    <div class="control">
      <label>Quick actions</label>
      <label style="font-weight: 400;"><input id="showPointLabels" type="checkbox"> Show point labels</label>
      <button id="useMaxLatency">Use range max as evaluated latency</button>
      <button id="resetDefaults">Reset defaults</button>
    </div>
  </div>

  <div id="validation" class="error"></div>
  <div id="summary" class="summary"></div>
  <div id="plot"></div>

  <h2>Plotted data</h2>
  <table>
    <thead>
      <tr>
        <th>Combination</th>
        <th>Configured scale</th>
        <th>Max buckets</th>
        <th>Effective scale</th>
        <th>Downscaled?</th>
        <th>Buckets needed at configured scale</th>
        <th>Buckets needed at effective scale</th>
        <th>Relative error</th>
        <th>Approx error height</th>
      </tr>
    </thead>
    <tbody id="dataBody"></tbody>
  </table>

  <script>
    const config = $payload_json;

    const minInput = document.getElementById('minMs');
    const maxInput = document.getElementById('maxMs');
    const latencyInput = document.getElementById('latencyMs');
    const latencySlider = document.getElementById('latencySlider');
    const minScaleInput = document.getElementById('minScale');
    const maxScaleInput = document.getElementById('maxScale');
    const showPointLabelsInput = document.getElementById('showPointLabels');
    const validation = document.getElementById('validation');
    const summary = document.getElementById('summary');
    const dataBody = document.getElementById('dataBody');

    minInput.value = config.initialMinMs;
    maxInput.value = config.initialMaxMs;
    latencyInput.value = config.initialLatencyMs;
    latencySlider.value = config.initialLatencyMs;
    minScaleInput.value = config.initialMinScale;
    maxScaleInput.value = config.initialMaxScale;
    let previousGridShape = '';

    function formatMs(value) {
      if (!Number.isFinite(value)) return 'n/a';
      if (Math.abs(value) >= 100) return value.toFixed(3).replace(/0+$/, '').replace(/\.$/, '');
      return value.toFixed(5).replace(/0+$/, '').replace(/\.$/, '');
    }

    function bucketFactor(scale) {
      return Math.pow(2, Math.pow(2, -scale));
    }

    function relativeError(scale) {
      const factor = bucketFactor(scale);
      return (factor - 1) / (factor + 1);
    }

    function bucketsNeeded(scale, minMs, maxMs) {
      if (maxMs === minMs) return 1;
      return Math.ceil(Math.log2(maxMs / minMs) * Math.pow(2, scale));
    }

    function effectiveScale(configuredScale, maxBuckets, minMs, maxMs) {
      for (let scale = configuredScale; scale >= 0; scale--) {
        if (bucketsNeeded(scale, minMs, maxMs) <= maxBuckets) return scale;
      }
      return 0;
    }

    function readState() {
      return {
        minMs: Number(minInput.value),
        maxMs: Number(maxInput.value),
        latencyMs: Number(latencyInput.value),
        minScale: Number.parseInt(minScaleInput.value, 10),
        maxScale: Number.parseInt(maxScaleInput.value, 10),
      };
    }

    function validateState(state) {
      if (!Number.isFinite(state.minMs) || state.minMs < config.minSupportedLatencyMs) return `Smallest positive latency must be >= ${config.minSupportedLatencyMs} ms.`;
      if (!Number.isFinite(state.maxMs) || state.maxMs > config.maxSupportedLatencyMs) return `Largest positive latency must be <= ${config.maxSupportedLatencyMs} ms.`;
      if (state.maxMs < state.minMs) return 'Largest positive latency must be >= smallest positive latency.';
      if (!Number.isFinite(state.latencyMs) || state.latencyMs < config.minSupportedLatencyMs || state.latencyMs > config.maxSupportedLatencyMs) {
        return `Latency to evaluate must be between ${config.minSupportedLatencyMs} ms and ${config.maxSupportedLatencyMs} ms.`;
      }
      if (!Number.isInteger(state.minScale) || state.minScale < 3 || state.minScale > 6) return 'Min configured max.scale must be an integer from 3 through 6.';
      if (!Number.isInteger(state.maxScale) || state.maxScale < 3 || state.maxScale > 6) return 'Max configured max.scale must be an integer from 3 through 6.';
      if (state.maxScale < state.minScale) return 'Max configured max.scale must be >= min configured max.scale.';
      return '';
    }

    function selectedScales(state) {
      const scales = [];
      for (let scale = state.minScale; scale <= state.maxScale; scale++) {
        scales.push(scale);
      }
      return scales;
    }

    function syncSliderToRange(state) {
      latencySlider.min = state.minMs;
      latencySlider.max = state.maxMs;
      latencySlider.step = Math.max((state.maxMs - state.minMs) / 1000, 0.000001);
      if (state.latencyMs < state.minMs) {
        latencyInput.value = state.minMs;
        latencySlider.value = state.minMs;
      } else if (state.latencyMs > state.maxMs) {
        latencyInput.value = state.maxMs;
        latencySlider.value = state.maxMs;
      } else {
        latencySlider.value = state.latencyMs;
      }
    }

    function buildRows(state) {
      const rows = [];
      for (const configuredScale of selectedScales(state)) {
        for (const maxBuckets of config.buckets) {
          const effScale = effectiveScale(configuredScale, maxBuckets, state.minMs, state.maxMs);
          const relErr = relativeError(effScale);
          rows.push({
            configuredScale,
            maxBuckets,
            effectiveScale: effScale,
            downscaled: effScale !== configuredScale,
            configuredBucketsNeeded: bucketsNeeded(configuredScale, state.minMs, state.maxMs),
            effectiveBucketsNeeded: bucketsNeeded(effScale, state.minMs, state.maxMs),
            relativeErrorPercent: relErr * 100,
            errorMs: state.latencyMs * relErr,
          });
        }
      }
      return rows;
    }

    function buildZMatrix(rows, state) {
      return selectedScales(state).map(scale =>
        config.buckets.map(bucket =>
          rows.find(row => row.configuredScale === scale && row.maxBuckets === bucket).errorMs
        )
      );
    }

    function renderTable(rows) {
      dataBody.innerHTML = rows.map(row => `
        <tr>
          <td>scale=${row.configuredScale}, buckets=${row.maxBuckets}</td>
          <td>${row.configuredScale}</td>
          <td>${row.maxBuckets}</td>
          <td>${row.effectiveScale}</td>
          <td class="${row.downscaled ? 'downscaled' : 'preserved'}">${row.downscaled ? 'yes' : 'no'}</td>
          <td>${row.configuredBucketsNeeded}</td>
          <td>${row.effectiveBucketsNeeded}</td>
          <td>±${row.relativeErrorPercent.toFixed(6)}%</td>
          <td>±${formatMs(row.errorMs)} ms</td>
        </tr>
      `).join('');
    }

    function render() {
      let state = readState();
      const validationMessage = validateState(state);
      validation.textContent = validationMessage;
      if (validationMessage) return;

      syncSliderToRange(state);
      state = readState();

      const rows = buildRows(state);
      const title = `Error height at ${formatMs(state.latencyMs)} ms for range ${formatMs(state.minMs)}–${formatMs(state.maxMs)} ms`;
      const zMatrix = buildZMatrix(rows, state);
      const scales = selectedScales(state);
      const pointX = rows.map(row => row.maxBuckets);
      const pointY = rows.map(row => row.configuredScale);
      const pointZ = rows.map(row => row.errorMs);
      const pointColors = rows.map(row => row.downscaled ? '#d62728' : '#2ca02c');
      const hoverText = rows.map(row => [
        `configured scale: ${row.configuredScale}`,
        `max buckets: ${row.maxBuckets}`,
        `effective scale: ${row.effectiveScale}`,
        `downscaled: ${row.downscaled}`,
        `buckets needed at configured scale: ${row.configuredBucketsNeeded}`,
        `buckets needed at effective scale: ${row.effectiveBucketsNeeded}`,
        `relative error: ±${row.relativeErrorPercent.toFixed(6)}%`,
        `latency: ${formatMs(state.latencyMs)} ms`,
        `approx error: ±${formatMs(row.errorMs)} ms`,
      ].join('<br>'));

      const surface = {
        type: 'surface',
        x: config.buckets,
        y: scales,
        z: zMatrix,
        colorscale: 'Viridis',
        opacity: 0.78,
        contours: { z: { show: true, usecolormap: true, highlightcolor: '#333', project: { z: true } } },
        hovertemplate: 'max.buckets=%{x}<br>configured scale=%{y}<br>error height=±%{z:.6f} ms<extra></extra>'
      };

      const points = {
        type: 'scatter3d',
        mode: showPointLabelsInput.checked ? 'markers+text' : 'markers',
        x: pointX,
        y: pointY,
        z: pointZ,
        text: pointZ.map(value => '±' + value.toFixed(5) + ' ms'),
        textposition: 'top center',
        marker: { size: 7, color: pointColors, line: { color: '#111', width: 1 } },
        hovertext: hoverText,
        hoverinfo: 'text'
      };

      const layout = {
        title,
        scene: {
          xaxis: { title: 'max.buckets', tickmode: 'array', tickvals: config.buckets },
          yaxis: { title: 'configured max.scale', tickmode: 'array', tickvals: scales },
          zaxis: { title: 'approx bucket-level error height (ms)' }
        },
        margin: { l: 0, r: 0, b: 0, t: 50 }
      };

      summary.innerHTML = `Selected positive range: <strong>${formatMs(state.minMs)}–${formatMs(state.maxMs)} ms</strong>. `
        + `Evaluated latency: <strong>${formatMs(state.latencyMs)} ms</strong>. `
        + `Configured scale range: <strong>${state.minScale}–${state.maxScale}</strong>. `
        + `Green = configured scale preserved; red = downscaled because bucket cap is too low for the selected range.`;
      renderTable(rows);

      const gridShape = `${scales.length}x${config.buckets.length}`;
      if (gridShape !== previousGridShape) {
        Plotly.purge('plot');
        Plotly.newPlot('plot', [surface, points], layout, { responsive: true });
        previousGridShape = gridShape;
      } else {
        Plotly.react('plot', [surface, points], layout, { responsive: true });
      }
    }

    minInput.addEventListener('input', render);
    maxInput.addEventListener('input', render);
    latencyInput.addEventListener('input', () => {
      latencySlider.value = latencyInput.value;
      render();
    });
    latencySlider.addEventListener('input', () => {
      latencyInput.value = latencySlider.value;
      render();
    });
    minScaleInput.addEventListener('input', render);
    maxScaleInput.addEventListener('input', render);
    showPointLabelsInput.addEventListener('change', render);
    document.getElementById('useMaxLatency').addEventListener('click', () => {
      latencyInput.value = maxInput.value;
      latencySlider.value = maxInput.value;
      render();
    });
    document.getElementById('resetDefaults').addEventListener('click', () => {
      minInput.value = config.initialMinMs;
      maxInput.value = config.initialMaxMs;
      latencyInput.value = config.initialLatencyMs;
      latencySlider.value = config.initialLatencyMs;
      minScaleInput.value = config.initialMinScale;
      maxScaleInput.value = config.initialMaxScale;
      showPointLabelsInput.checked = false;
      render();
    });

    render();
  </script>
</body>
</html>
"""
    )

    return template.safe_substitute(
        title=html.escape(title),
        payload_json=json.dumps(payload),
        plotly_vendor_path=html.escape(PLOTLY_VENDOR_PATH),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate an interactive range-aware 3D OTel histogram accuracy plot.")
    parser.add_argument("--min-ms", type=float, default=0.05, help="Initial smallest positive latency in milliseconds")
    parser.add_argument("--max-ms", type=float, default=150.0, help="Initial largest positive latency in milliseconds")
    parser.add_argument("--latency-ms", type=float, default=150.0, help="Initial latency value to evaluate")
    parser.add_argument("--scales", default="3,4,5,6", help="Comma-separated configured max.scale values")
    parser.add_argument(
        "--buckets",
        default="250,300,350,370,450,500,600,650,700,740,800,900,1000",
        help="Comma-separated max.buckets values",
    )
    parser.add_argument(
        "--output",
        default="scripts/otel-accuracy-evaluation/otel_histogram_accuracy_range_3d_plot.html",
        help="HTML output path",
    )
    args = parser.parse_args()

    scales = parse_csv_numbers(args.scales, int)
    buckets = parse_csv_numbers(args.buckets, int)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        build_html(args.min_ms, args.max_ms, args.latency_ms, scales, buckets),
        encoding="utf-8",
    )
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()

