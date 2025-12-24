# Local Documentation Deployment

## Prerequisites

- Python 3.8+
- pip
- Git

## Quick Start

1. **Install dependencies:**
   ```bash
   pip install -r requirements-docs.txt
   ```

2. **Serve locally:**
   ```bash
   mkdocs serve
   ```
   
   Open [http://127.0.0.1:8000](http://127.0.0.1:8000)

3. **Build static site:**
   ```bash
   mkdocs build
   ```
   
   Output in `site/` directory.

## Options

**Custom port:**
```bash
mkdocs serve --dev-addr=127.0.0.1:8080
```

**With Javadoc:**
```bash
./gradlew aggregateJavadoc
mkdir -p docs/javadoc
cp -r build/javadoc/* docs/javadoc/
mkdocs serve
```

**Strict mode:**
```bash
mkdocs serve --strict
```

## Troubleshooting

**Module errors:**
```bash
pip install --upgrade -r requirements-docs.txt
```

**Port in use:**
```bash
mkdocs serve --dev-addr=127.0.0.1:8001
```
