
# Documentation Guideline

We use GitHub Pages to host Venice documentation with MkDocs Material theme. Documentation is built automatically by GitHub Actions when changes are pushed to the `main` branch.

## General

It is strongly encouraged that any code change which affects the validity of information in the docs also include 
updates to the docs, so that both are kept in sync atomically.

Experimental functionalities and future plans are also worth documenting, though they must be clearly marked as such, so
that users and operators reading those docs can make informed decisions about the level of risk they are willing to take
on if trying out a given functionality. If the level of maturity of a given functionality is not called out, then it
implicitly means that the functionality is considered mature and its API is unlikely to change. Undocumented configs and
APIs may or may not be considered mature and stable, and if in doubt, it is appropriate to open an Issue to request that
it be explicitly documented.

In general, it is recommended to get familiar with the docs before writing more docs, to try to keep the style and
structure coherent. That being said, even if unsure where some documentation belongs, do err on the side of including it
(anywhere), and reviewers may suggest placing it elsewhere.

## Hierarchy

Documentation hierarchy is configured in `mkdocs.yml` in the `nav:` section. Pages do not require front matter. The navigation structure is defined centrally:

```yaml
nav:
  - Home: index.md
  - User Guide:
    - user-guide/index.md
    - Write APIs:
      - user-guide/write-apis/batch-push.md
```

To add a new page:
1. Create the markdown file in the appropriate directory
2. Add an entry to the `nav:` section in `mkdocs.yml`
3. The page title is taken from the first `# Heading` in the markdown file

For more information, consult [MkDocs Material Documentation](https://squidfunk.github.io/mkdocs-material/).

## Pictures and Diagrams

It is encouraged to use diagrams within the documentation, but there are some guidelines to standardize the way it is
done, and to avoid certain anti-patterns.

### Text-based Assets

For text-based assets (which are preferred whenever feasible), we wish to check them into source control. This should 
include both the displayable asset (e.g. in SVG format) and the source file from which the displayable asset was 
generated (e.g. in XML format). This makes the docs self-contained, and enables contributors to edit the assets over 
time.

Diagrams conforming to these guidelines can be placed under the `/docs/assets/images` path of the repo, and then 
embedded in the docs with a relative link like this:

```markdown
![](../../assets/images/vip_3_read_path.drawio.svg)
```

The [draw.io](https://draw.io) service makes it easy to generate such assets. If using [PlantUML](https://plantuml.com/starting), 
it's recommended to generate diagrams into svg format by following this [guide](https://plantuml.com/svg).

### Binary Assets

For binary assets (e.g. PNG, BMP, JPG, etc.), we do NOT wish to check them into source control. Instead, they should be 
linked from an external source. This can be done in GitHub itself. Within the Pull Request that proposes the doc change, 
the contributor can insert images in the PR's description or comments, and then take the URL GitHub generated for it. 
Then the modified files included in the PR can be edited to link to that image, and the PR updated. Externally hosted 
images can be embedded with an absolute link like this:

```markdown
![](https://user-images.githubusercontent.com/1248632/195111861-518f81c4-f226-4942-b88a-a34337da79e3.png)
```

## Emojis

Here's a link to all the emojis available in README files: [Emoji Cheat Sheet](https://github.com/ikatyang/emoji-cheat-sheet/blob/master/README.md). 

## Testing Doc Changes

There are two ways to test doc changes, locally and on the public web. Local testing is convenient to iterate quickly,
while public web testing is useful to make sure that nothing breaks (e.g., especially if changing styles, Ruby 
dependencies, or Jekyll configs) and to share more significant documentation changes with PR reviewers.

### Testing Locally

The docs are rendered and served by MkDocs with the Material theme. Install dependencies:

```bash
pip install mkdocs-material 
```

Then from the repository root, run:

```bash
mkdocs serve
```

Then navigate to `http://127.0.0.1:8000` and view the docs in your browser. MkDocs hot reloads changes to markdown files and configuration automatically. If you modify `mkdocs.yml`, the server will restart automatically.

For more options, see `docs/LOCAL_DEPLOYMENT.md`.

### Testing on the Public Web

A GitHub fork can have its own documentation. This can be setup by:

1. Navigating to the fork's Settings > Pages, i.e.: `https://github.com/<username>/venice/settings/pages`
2. Selecting which branch to publish the docs from
3. Selecting "GitHub Actions" as the source (not "Deploy from a branch")
4. Push changes to trigger the `deploy-docs.yml` workflow
5. Navigate to your fork's docs at: `https://<username>.github.io/venice`

For significant doc changes, please follow this process and add a link inside the PR to the docs hosted in the PR author's own fork.