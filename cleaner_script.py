import os
import re

# Emoji to text mapping
EMOJI_MAP = {
    '🔍': '[DEBUG]',
    '✅': '[OK]',
    '⚠️': '[WARN]',
    '❌': '[ERROR]',
    '📊': '[DATA]',
    '📚': '',
    '💾': '',
    '🌍': '',
    '👥': '',
    '📂': '',
    '📦': '',
    '🗺️': ''
}

def remove_emojis_from_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    for emoji, replacement in EMOJI_MAP.items():
        content = content.replace(emoji, replacement)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"Cleaned: {filepath}")

# Clean all Python files
for root, dirs, files in os.walk('app'):
    for file in files:
        if file.endswith('.py'):
            filepath = os.path.join(root, file)
            remove_emojis_from_file(filepath)

print("Done!")