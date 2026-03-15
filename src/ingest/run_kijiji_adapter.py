from src.ingest.kijiji_adapter import KijijiAdapter

if __name__ == "__main__":
    adapter = KijijiAdapter(
        search_term="milwaukee drill",
        search_region="calgary"
    )
    adapter.run()