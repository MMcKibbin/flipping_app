import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.ingest.kijiji_adapter import KijijiAdapter

adapter = KijijiAdapter(search_term="cordless drill", search_region="calgary")
html = adapter.fetch_page()

out_path = Path("data/raw/kijiji")
out_path.mkdir(parents=True, exist_ok=True)

file_path = out_path / "kijiji_cordless_drill_calgary.html"
file_path.write_text(html, encoding="utf-8")

print(f"[OK] Saved HTML to: {file_path}")