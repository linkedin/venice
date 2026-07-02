# Vendored Plotly.js

This directory contains a vendored copy of Plotly.js used by the OTEL histogram accuracy visualization so the generated
HTML can run without loading JavaScript from a CDN.

Vendored artifact:

- `plotly-2.35.2.min.js`
- Source: `https://cdn.plot.ly/plotly-2.35.2.min.js`
- Version: `2.35.2`
- License: MIT

License and notice files:

- `plotly-2.35.2.LICENSE`
  - Plotly.js MIT license.
- `plotly.min.js.LICENSE.txt`
  - License notices referenced by the minified Plotly bundle for bundled dependencies.

When updating Plotly, update the JavaScript bundle and both license/notice files.
