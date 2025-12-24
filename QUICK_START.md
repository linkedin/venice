# MkDocs Quick Reference

## 🚀 Deploy Now (3 Commands)

```bash
# 1. Stage all changes
git add mkdocs.yml requirements-docs.txt .github/ docs/ scripts/ *.md

# 2. Commit
git commit -m "Migrate docs to MkDocs Material with dark mode and ML emphasis"

# 3. Push (auto-deploys to venicedb.org)
git push origin main
```

---

## 💻 Local Development

```bash
# Install dependencies (one time)
pip install -r requirements-docs.txt

# Serve locally (auto-reload on changes)
mkdocs serve
# → http://127.0.0.1:8000

# Build static site
mkdocs build
# → Output in site/

# Build with validation
mkdocs build --strict
```

---

## 📊 What Changed

| Aspect | Before | After |
|--------|--------|-------|
| **Theme** | Jekyll (Just the Docs) | MkDocs (Material) |
| **Colors** | Light mode default | Dark mode default, Venice black/white |
| **Structure** | Mixed user/dev docs | Clear: Users / Operators / Contributors |
| **Navigation** | Sidebar only | Tabs + sidebar + search + breadcrumbs |
| **Build** | Ruby/Jekyll | Python/MkDocs |
| **Deploy** | Manual | Automated via GitHub Actions |
| **Copyright** | Static year | Dynamic (auto-updates) |
| **ML Focus** | Mentioned | Prominently featured on landing |

---

## 📁 New Structure

```
docs/
├── index.md                    ← Landing (ML feature store + derived data)
├── getting-started/            ← Quickstarts
├── user-guide/                 ← For USERS (concepts, APIs, patterns)
├── operations/                 ← For OPERATORS (deployment, management)
├── contributing/               ← For CONTRIBUTORS (dev, architecture, VIPs)
└── resources/                  ← Learn more, API reference, community
```

**48 markdown files** organized into **23 navigation sections** across **22 directories**

---

## ✅ Verification

```bash
# Check build status
mkdocs build 2>&1 | tail -1
# Should show: "Documentation built in X.XX seconds"

# Count pages
find docs -name "*.md" | wc -l
# Should show: 48

# Preview specific page
mkdocs serve
# Then visit: http://127.0.0.1:8000/user-guide/
```

---

## 🎨 Customization

**Edit theme colors** → `mkdocs.yml` (theme palette section)
**Edit CSS** → `docs/assets/style/extra.css`
**Edit navigation** → `mkdocs.yml` (nav section)
**Edit landing page** → `docs/index.md`

---

## 🔧 Troubleshooting

| Issue | Solution |
|-------|----------|
| Port 8000 in use | `mkdocs serve --dev-addr=127.0.0.1:8001` |
| Broken links | `mkdocs build --strict` (shows all warnings) |
| Module import error | `pip install --upgrade -r requirements-docs.txt` |
| Deploy failed | Check https://github.com/linkedin/venice/actions |

---

## 📚 Documentation

- **MkDocs**: https://www.mkdocs.org/
- **Material Theme**: https://squidfunk.github.io/mkdocs-material/
- **Markdown Guide**: https://www.markdownguide.org/

---

## 🎯 Key Features Enabled

✅ Dark mode by default (Venice black/white colors)
✅ Instant page loading with progress bar
✅ Full-text search with suggestions
✅ Code copy buttons
✅ Tabbed content support
✅ Mermaid diagram support
✅ Mobile responsive design
✅ Edit on GitHub links
✅ Auto-generated last modified dates
✅ Minified HTML/CSS/JS for performance
✅ Dynamic copyright year (2022-2025)

---

## 🚦 Status: READY TO DEPLOY

Everything is configured and tested. Simply push to main branch and GitHub Actions will handle deployment to https://venicedb.org
