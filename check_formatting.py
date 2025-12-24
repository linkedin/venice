#!/usr/bin/env python3
import re
from pathlib import Path

docs_dir = Path('docs')
issues = []

for md_file in docs_dir.rglob('*.md'):
    if 'backup' in str(md_file):
        continue
        
    with open(md_file, 'r', encoding='utf-8') as f:
        content = f.read()
        lines = content.split('\n')
    
    # Check for unclosed code blocks
    code_block_count = content.count('```')
    if code_block_count % 2 != 0:
        issues.append(f'{md_file}: Unclosed code block ({code_block_count} backticks)')
    
    # Check for horizontal rules without proper spacing
    for i, line in enumerate(lines, 1):
        if line.strip() == '---':
            if i > 1 and i <= len(lines):
                prev_line = lines[i-2] if i > 1 else ''
                next_line = lines[i] if i < len(lines) else ''
                if prev_line.strip() and not prev_line.strip().startswith('#'):
                    issues.append(f'{md_file}:{i}: Horizontal rule may need blank line before')
                if next_line.strip() and not next_line.strip().startswith('#'):
                    issues.append(f'{md_file}:{i}: Horizontal rule may need blank line after')

print("Formatting Issues Found:")
print("=" * 60)
for issue in issues[:30]:
    print(issue)
    
if len(issues) > 30:
    print(f"\n... and {len(issues) - 30} more issues")
    
print(f'\nTotal: {len(issues)} potential issues')
