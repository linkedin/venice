#!/bin/bash

# Remove Jekyll front matter from all markdown files

echo "Removing Jekyll front matter from markdown files..."

find docs -name "*.md" -type f | while read file; do
  # Use perl for cross-platform compatibility
  perl -i -pe 'BEGIN{undef $/;} s/^---\n.*?\n---\n//sm' "$file"
  echo "Processed: $file"
done

echo "Front matter removal complete."
