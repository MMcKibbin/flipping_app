from src.ingest.facebook_adapter import FacebookAdapter

if __name__ == "__main__":
    adapter = FacebookAdapter(
        search_term="milwaukee drill",
        search_region="calgary"
    )
    adapter.run()