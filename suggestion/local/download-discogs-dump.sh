#!/bin/bash
set -e

LATEST_URL="https://data.discogs.com/?download=data%2F$(date +%Y)%2Fdiscogs_$(date +%Y%m%d)_releases.xml.gz"
SYMLINK="discogs_releases.xml.gz"

cd "$(dirname "$0")"

# Check if we already have a database
if command -v psql &> /dev/null; then
    DB_EXISTS=$(psql "$DATABASE_URL" -t -c "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'releases')" 2>/dev/null || echo "false")
    if [ "$DB_EXISTS" = "t" ]; then
        COUNT=$(psql "$DATABASE_URL" -t -c "SELECT COUNT(*) FROM releases" 2>/dev/null || echo "0")
        if [ "$COUNT" -gt 1000000 ] 2>/dev/null; then
            echo "Database already has $COUNT releases imported."
            echo "Run again next month for an update."
            echo ""
            echo "To re-import from scratch:"
            echo "  ./discogs-xml-to-pg.py $SYMLINK"
            echo ""
            echo "To clear and re-import:"
            echo "  psql \$DATABASE_URL -c 'DROP TABLE IF EXISTS releases, artists, tracks, release_artists, release_extraartists, track_artists CASCADE'"
            echo "  ./discogs-xml-to-pg.py $SYMLINK"
            exit 0
        fi
    fi
fi

# Check if we already have the file
if [ -f "$SYMLINK" ]; then
    TARGET=$(readlink "$SYMLINK" 2>/dev/null || echo "$SYMLINK")
    echo "Already have $SYMLINK -> $TARGET"
    echo ""
    echo "To re-download:"
    echo "  rm $SYMLINK && ./download-discogs-dump.sh"
    exit 0
fi

echo "Downloading Discogs release dump..."
echo "  URL: $LATEST_URL"

FILENAME="discogs_$(date +%Y%m%d)_releases.xml.gz"

if command -v curl &> /dev/null; then
    curl -L -o "$FILENAME" "$LATEST_URL"
elif command -v wget &> /dev/null; then
    wget -O "$FILENAME" "$LATEST_URL"
else
    echo "Error: need curl or wget" >&2
    exit 1
fi

ln -sf "$FILENAME" "$SYMLINK"
echo "Saved as $FILENAME"
echo "Symlinked as $SYMLINK"
echo ""
echo "To import into PostgreSQL:"
echo "  gunzip -c $SYMLINK > ${SYMLINK%.gz}"
echo "  ./discogs-xml-to-pg.py ${SYMLINK%.gz}"
