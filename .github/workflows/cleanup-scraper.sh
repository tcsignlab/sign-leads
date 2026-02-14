#!/bin/bash

# ============================================
# CLEANUP SCRIPT - Sign Leads Scraper
# Deletes old state pages and triggers fresh scrape
# ============================================

echo "🧹 Sign Leads Cleanup Script"
echo "============================"
echo ""

# Check if we're in a git repo
if [ ! -d .git ]; then
    echo "❌ Error: Not in a git repository"
    echo "   Run this script from your repo root"
    exit 1
fi

# Check if state-pages directory exists
if [ ! -d "state-pages" ]; then
    echo "📁 No state-pages directory found (that's okay)"
else
    echo "📁 Found state-pages directory"
    
    # Count existing HTML files
    html_count=$(ls state-pages/*.html 2>/dev/null | wc -l)
    echo "   Found $html_count HTML files"
    
    if [ $html_count -gt 0 ]; then
        echo ""
        read -p "❓ Delete all $html_count state HTML files? (y/n): " -n 1 -r
        echo ""
        
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            echo "🗑️  Deleting old state pages..."
            rm -f state-pages/*.html
            echo "✅ Deleted $html_count files"
        else
            echo "⏭️  Skipping deletion"
        fi
    fi
fi

# Check if summary file exists
if [ -f "state-pages/scrape-summary.json" ]; then
    echo ""
    read -p "❓ Delete old scrape-summary.json? (y/n): " -n 1 -r
    echo ""
    
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        rm -f state-pages/scrape-summary.json
        echo "✅ Deleted scrape-summary.json"
    fi
fi

echo ""
echo "📝 Git Status:"
git status --short

echo ""
read -p "❓ Commit and push cleanup? (y/n): " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "📤 Committing changes..."
    git add state-pages/
    git commit -m "🧹 Cleanup: Remove old state pages for fresh scrape"
    
    echo "📤 Pushing to GitHub..."
    git push origin main
    
    echo ""
    echo "✅ CLEANUP COMPLETE!"
    echo ""
    echo "🚀 NEXT STEPS:"
    echo "   1. Go to: https://github.com/$(git config --get remote.origin.url | sed 's/.*github.com[:/]\(.*\)\.git/\1/')/actions"
    echo "   2. Click 'Sign Lead Scraper' workflow"
    echo "   3. Click 'Run workflow' button"
    echo "   4. Wait ~30-45 minutes for all states to complete"
    echo ""
else
    echo "⏭️  Skipped commit"
    echo ""
    echo "💡 TIP: Manually commit when ready:"
    echo "   git add state-pages/"
    echo "   git commit -m 'Cleanup old state pages'"
    echo "   git push"
fi

echo ""
echo "Done! 🎉"
