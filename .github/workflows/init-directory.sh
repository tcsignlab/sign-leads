#!/bin/bash

# ============================================
# Initialize state-pages directory
# Run this ONCE before first scraper run
# ============================================

echo "📁 Creating state-pages directory..."

# Create directory
mkdir -p state-pages

# Create .gitkeep to ensure directory is tracked by git
echo "# This file ensures the state-pages directory is tracked by git" > state-pages/.gitkeep

# Add to git
git add state-pages/.gitkeep
git commit -m "📁 Initialize state-pages directory"

echo "✅ Done! Directory created and committed."
echo ""
echo "Next steps:"
echo "1. git push"
echo "2. Run your scraper (GitHub Actions or locally)"
echo "3. State pages will be created in this directory"
