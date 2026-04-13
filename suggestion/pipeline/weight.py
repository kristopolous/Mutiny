#!/usr/bin/env python3
"""
Weight releases by similarity using IDF-style weighting.

High cardinality (common) properties = weak signal (low weight)
Low cardinality (rare) properties = strong signal (high weight)

This is fundamentally Bayesian - rare co-occurrences carry more information.
"""
import sys
import json
import math
from collections import Counter, defaultdict
from dotenv import load_dotenv
load_dotenv()

# No imports needed - data comes from traverse.py


def compute_weights(releases, source_release_ids):
    """
    Compute weights for discovered releases using IDF-style weighting.
    
    Uses data already provided by traverse.py (no re-fetching needed).
    """
    print(f"Computing weights for {len(releases)} releases...", file=sys.stderr)
    
    # Get all features from release (contributors + labels + extraartists)
    def get_features(release):
        features = set()
        for c in release.get("contributors", []):
            if c and c != "0":
                features.add(f"artist:{c}")
        for ea in release.get("extraartists", []):
            if ea and ea != "0":
                features.add(f"artist:{ea}")
        for l in release.get("labels", []):
            if l and l != "0":
                features.add(f"label:{l}")
        return features
    
    # Separate helpers for diagnostics
    def get_contributors(release):
        contribs = set()
        for c in release.get("contributors", []):
            if c and c != "0":
                contribs.add(c)
        for ea in release.get("extraartists", []):
            if ea and ea != "0":
                contribs.add(ea)
        return contribs
    
    def get_labels(release):
        return {l for l in release.get("labels", []) if l and l != "0"}
    
    # Get source release data from input - convert to strings for consistent comparison
    source_ids_set = set(str(sid) for sid in source_release_ids)
    source_releases = [r for r in releases if r.get("release_id") in source_ids_set]
    
    source_features = set()
    source_contributors = set()
    source_labels = set()
    for r in source_releases:
        source_features.update(get_features(r))
        source_contributors.update(get_contributors(r))
        source_labels.update(get_labels(r))
    
    print(f"  Source has {len(source_features)} features ({len(source_contributors)} contributors, {len(source_labels)} labels)", file=sys.stderr)
    
    # Build cardinality map for all features
    print("Computing cardinality...", file=sys.stderr)
    
    feature_counts = Counter()
    for r in releases:
        for f in get_features(r):
            feature_counts[f] += 1
    
    print(f"  Found {len(feature_counts)} unique features", file=sys.stderr)
    
    # IDF = 1 / log(1 + count) - higher count = lower weight
    feature_idf = {f: 1.0 / math.log(1 + cnt) for f, cnt in feature_counts.items()}
    
    # Compute weight for each discovered release
    results = []
    
    for release in releases:
        rid = release.get("release_id")
        if not rid or rid in source_ids_set:
            continue
        
        release_features = get_features(release)
        release_contributors = get_contributors(release)
        release_labels = get_labels(release)
        
        shared_features = source_features & release_features
        shared_contributors = source_contributors & release_contributors
        shared_labels = source_labels & release_labels
        
        # Compute IDF weights for each category
        contrib_weight = sum(feature_idf.get(f"artist:{c}", 0) for c in shared_contributors)
        label_weight = sum(feature_idf.get(f"label:{l}", 0) for l in shared_labels)
        
        # Final weight is the combined score
        final_weight = contrib_weight + label_weight
        
        if final_weight > 0:
            results.append({
                **release,
                "final_weight": round(final_weight, 4),
                # Diagnostic fields - useful for debugging
                "contrib_weight": round(contrib_weight, 4),
                "label_weight": round(label_weight, 4),
                "shared_contributors": list(shared_contributors),
                "shared_labels": list(shared_labels),
                "shared_count": len(shared_features),
            })
    
    results.sort(key=lambda x: -x.get("final_weight", 0))
    print(f"  Weighted {len(results)} releases", file=sys.stderr)
    
    return results


def extract_release_id_from_url(url):
    """Extract release ID from Discogs URL."""
    import re
    match = re.search(r'/release/(\d+)', url)
    if match:
        return match.group(1)
    return None


def main():
    # Get source URL from command line args (optional, overrides stdin)
    source_url = None
    if len(sys.argv) > 1:
        source_url = sys.argv[1]
        print(f"Using source URL: {source_url}", file=sys.stderr)
    
    # Read JSON from stdin (traverse.py output)
    try:
        input_data = json.load(sys.stdin)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON input: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Handle both old format (flat array) and new format (object with source_ids)
    if isinstance(input_data, dict) and "releases" in input_data:
        releases = input_data["releases"]
        source_ids = input_data.get("source_ids", [])
    elif isinstance(input_data, list):
        releases = input_data
        # Extract source IDs from discogs_url for backward compatibility
        source_ids = []
        for r in releases:
            url = r.get("discogs_url", "")
            if "/release/" in url:
                rid = url.split("/release/")[-1]
                source_ids.append(rid)
    else:
        print("Error: Expected JSON array or object with releases key", file=sys.stderr)
        sys.exit(1)
    
    # If source URL provided, use it to determine source IDs (overrides stdin data)
    if source_url:
        source_ids = [extract_release_id_from_url(source_url)]
        print(f"Using source from URL: {source_ids}", file=sys.stderr)
    
    if not source_ids:
        print("Error: Could not find source release IDs", file=sys.stderr)
        sys.exit(1)
    
    print(f"Source releases: {source_ids}", file=sys.stderr)
    
    # Compute weights
    weighted = compute_weights(releases, source_ids)
    
    # Output JSON with metadata
    output = {
        "source_ids": source_ids,
        "releases": weighted
    }
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()