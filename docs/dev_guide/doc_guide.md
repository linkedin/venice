---
layout: default
title: Documentation Guideline
parent: Developer Guides
permalink: /docs/dev_guide/documentation_guideline
---

# Documentation Guideline

We prefer simplicity and currently use GitHub page to host Venice documentation. Those documentation will be built automatically by GitHub pipelines in the `main` branch.

## Hierarchy

In order for your docs to be rendered properly in the documentation hierarchy, Venice developers need to add a header
section at the top of each documentation. The `title` section will be what the end user sees in the sidebar, and
the `parent` section represents the parent page of the current page for linking purpose. The `permalink` section will be
the URL path where the page will be served. An example of the header is below:

```
---
layout: default
title: Documentation Guideline
parent: Developer Guides
permalink: /docs/dev_guide/documentation_guideline
---
```

## Emojis

Here's a link to all the emojis available in README files: [Emoji Cheat Sheet](https://github.com/ikatyang/emoji-cheat-sheet/blob/master/README.md). If you want to find a good emoji, you can use [this website](https://emojicombos.com/).