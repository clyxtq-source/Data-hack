# Sydney Traffic

A local FastAPI web app for exploring Sydney travel routes with driving, walking,
transit, traffic-count, and weather context.

## Project Structure

- `app.py` - FastAPI backend and routing logic.
- `static/` - frontend HTML, CSS, and JavaScript.
- `data/` - prebuilt Sydney graph/network data.
- `road_traffic_counts_hourly_sample_0.csv` - traffic-count sample data.
- `requirements.txt` - Python dependencies.
- `START_APP.bat` / `START_APP.ps1` - local startup scripts.
- `.env.example` - example environment variable file.

## Setup

1. Install Python 3.12 or newer.
2. Install dependencies:

```powershell
python -m pip install -r requirements.txt
```

3. Copy `.env.example` to `.env`.
4. Add your Transport for NSW API key:

```text
TFNSW_API_KEY=your_api_key_here
```

5. Start the app:

```powershell
.\START_APP.ps1
```

6. Open `http://127.0.0.1:8000`.

## GitHub Notes

This repository uses Git LFS for large data files such as `.graphml`, `.geojson`,
and `.csv`. Install Git LFS before committing the data files:

```powershell
git lfs install
```

Do not commit `.env`. Use `.env.example` to show which environment variables are
needed.

Generated cache files and Python bytecode are ignored by `.gitignore`.

## Notes

- The first startup can take time because the app preloads large Sydney routing
  assets.
- Keep the terminal window open while the website is running.
- If port `8000` is busy, stop the old server first, then run the start script
  again.
