# 🎬 Movie Agent

> A Python-powered movie recommendation engine with a Bayesian scoring algorithm, REST API, and JWT authentication.

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat&logo=python&logoColor=white)
![Flask](https://img.shields.io/badge/Flask-REST%20API-000000?style=flat&logo=flask)
![JWT](https://img.shields.io/badge/Auth-JWT-000000?style=flat&logo=jsonwebtokens)
![Pandas](https://img.shields.io/badge/Data-Pandas%20%2B%20Parquet-150458?style=flat&logo=pandas)
![IMDB](https://img.shields.io/badge/Data-IMDB%20Dataset-F5C518?style=flat&logo=imdb)

---

## What is it?

Movie Agent pulls from the IMDB public dataset and scores every movie using a **Bayesian averaging algorithm** — the same approach used by IMDB's own Top 250 list. It corrects for vote count bias, so a 9.0 from 50 votes doesn't outrank an 8.5 from 50,000.

Recommendations are served through a **Flask REST API** with JWT-based authentication, and filtered by genre, rating, or release year per request. A CLI interface is also available for local use.

---

## Architecture

```
AppManager                        ← Orchestrates CLI vs API entry points
  └── MovieService                ← Core business logic, interface-agnostic
        ├── MovieAgent            ← DataFrame container
        │     └── DataPipeline    ← Load, merge, cache IMDB TSV/Parquet data
        │           └── DataLoader
        ├── MovieScorer           ← Bayesian scoring (scorer/bayesian_algorithm.py)
        ├── MovieFilter           ← Filter and rank candidate DataFrame
        └── StateStore            ← Persist recommendation history (Parquet)
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.10+ |
| API | Flask |
| Data processing | Pandas, PyArrow, Parquet |
| Dataset | IMDB public TSV datasets |
| Authentication | PyJWT, bcrypt |
| HTTP client | Requests |
| Persistence | Parquet files |
| CLI | Custom terminal UI |

### Dependencies

**Third-party:** Flask, PyJWT, bcrypt, pandas, pyarrow, requests, tqdm

**Standard library:** pathlib, gzip, json, logging, datetime, os

---

## ETL Pipeline

IMDB distributes its dataset as gzip-compressed TSV files. On first run, Movie Agent:

1. **Streams** the compressed files from IMDB using `requests` — no full download into memory
2. **Decompresses** on the fly with `gzip`
3. **Parses and merges** multiple TSV files into a single DataFrame with `pandas`
4. **Caches** the result as a Parquet file via `pyarrow` for fast subsequent loads

Progress is tracked with `tqdm`. On subsequent runs, the pipeline skips straight to loading from Parquet — significantly faster startup.

---

## API Endpoints

```
POST  /login            → Returns JWT access token + refresh token
POST  /refresh          → Exchanges refresh token for new access token
POST  /recommendations  → Returns scored, filtered movie list (protected)
GET   /health           → Service health check
```

All protected routes require `Authorization: Bearer <token>`.

---

## Auth Design

Three-layer security stack:

- **bcrypt** (cost factor 12) — password hashing. ~33 brute-force attempts/sec ceiling without rate limiting
- **JWT (HS256)** — self-verifying signed access tokens, 15-minute expiry, no DB lookup required per request
- **secrets.token_hex** — cryptographically random refresh tokens, 30-day expiry, server-side dictionary lookup

---

## Bayesian Scoring

Standard weighted rating formula:

```
Score = (v / (v + m)) × R + (m / (v + m)) × C
```

Where `v` = vote count, `m` = minimum votes threshold, `R` = movie average, `C` = global average. Scores are computed once at startup across the full dataset and held in memory.

---

## Folder Structure

```
movie-agent/
├── main.py               ← Core classes (MovieAgent, MovieService, AppManager...)
├── scorer/
│   └── bayesian_algorithm.py
├── persist/
│   └── state_store.py
├── api/
│   └── api.py            ← Flask server
├── downloader/
│   └── downloader.py     ← IMDB dataset streaming + decompression
├── ui/
│   └── cli.py
├── cons/
│   └── constants.py
├── config/               ← JSON config files
├── data/                 ← Parquet + TSV (gitignored)
└── logs/
```

---

## Getting Started

```bash
# Clone the repo
git clone https://github.com/bugra-ozer/movie-agent
cd movie-agent

# Install dependencies
pip install -r requirements.txt

# Set environment variables
cp .env.example .env  # add your SECRET_KEY

# Run the API
python api/api.py

# Or run the CLI
python main.py
```

---

## Roadmap

- [ ] Per-user recommendation history
- [ ] Rate limiting (Flask-Limiter)
- [ ] Role-based authorisation
- [ ] Replace hardcoded users with database (Flask-SQLAlchemy)
- [ ] HTTPS (Flask-Talisman)
- [ ] Multi-genre filtering
- [ ] OMDB API integration for live metadata
- [ ] OpenAPI / Swagger docs

---

## Author

**Bugra Ozer** — [github.com/bugra-ozer](https://github.com/bugra-ozer)