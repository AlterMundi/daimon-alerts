"""Exercise the selected DMK embedding space without submitting memory content."""
import argparse
import math
from pathlib import Path
import sqlite3
import sys


def check(runtime_dir, profile_path):
    sys.path.insert(0, str(runtime_dir))
    from memory_runtime import load_memory, profile

    settings = profile(profile_path)
    memory = load_memory(settings)
    space = memory.embeddings_runtime_config()
    for input_type in ("query", "passage"):
        vectors = memory.embed_texts(
            space["provider"], ["Synthetic memory service availability check."],
            input_type=input_type, model=space["model"],
            output_dimensionality=space["output_dimensionality"],
        )
        if len(vectors) != 1 or not vectors[0] or not all(map(math.isfinite, vectors[0])):
            return False
        dims = len(vectors[0])
        if space["output_dimensionality"] not in (None, dims):
            return False
        if input_type == "query":
            query_dims = dims
        elif dims != query_dims:
            return False

    with sqlite3.connect(Path(settings["database"]).as_uri() + "?mode=ro", uri=True) as db:
        populated = db.execute(
            "SELECT 1 FROM chapters WHERE embed_disabled=0 LIMIT 1"
        ).fetchone() is not None
        indexed_dims = {row[0] for row in db.execute(
            "SELECT DISTINCT dims FROM chapter_embeddings WHERE provider=? AND model=?",
            (space["provider"], space["model"]),
        )}
    return indexed_dims == {query_dims} or (not populated and not indexed_dims)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-scripts", type=Path, required=True)
    parser.add_argument("--profile", type=Path, required=True)
    args = parser.parse_args()
    try:
        return 0 if check(args.runtime_scripts, args.profile) else 1
    except (Exception, SystemExit):
        return 1


if __name__ == "__main__":
    sys.exit(main())
