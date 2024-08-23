# Tickr

**Tickr** is an open-source project designed to continuously fetch and update financial market data, starting with cryptocurrency candle data, using the powerful CCXT library. By leveraging GitHub Actions, Tickr automatically syncs the latest candle data at various granularities—ranging from 1 minute to 1 month—directly to this repository.

## Features

- **Automated Data Sync:** Tickr fetches and updates the latest market data at regular intervals using GitHub Actions.
- **Flexible Granularity:** Supports multiple timeframes, including 1 minute, 5 minutes, 15 minutes, 1 hour, 6 hours, 12 hours, 1 day, and 1 week.
- **Extensible Design:** Built to easily extend support to other assets with ticker symbols.
- **GitHub Integration:** Uses PyGithub for efficient data retrieval without needing to clone the entire repository.

## Getting Started

### Prerequisites

- Python 3.x
- GitHub account
- GitHub Personal Access Token (for PyGithub)

### Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/your_username/tickr.git
   cd tickr
   ```

2. Install the required Python packages:

   ```bash
   pip install -r requirements.txt
   ```
### Usage

#### Using the Tickr Client

The **TickrClient** class provides a high-level interface to access candle data. By default, it fetches the last 100 candles for the specified symbol and timeframe. You can also specify a custom date range.

**Note:** The maximum supported timeframe is 1 week (`1w`). The 1 month (`1M`) timeframe is not supported.

Example usage:

```python
from tickr_client import TickrClient

# Initialize TickrClient
client = TickrClient()

# Fetch the last 100 candles for the specified timeframe
symbol = "BTC/USDT"
timeframe = "1h"

candles = client.get_candles(symbol, timeframe)

if candles:
    print(f"Retrieved {len(candles)} candles for {symbol} - {timeframe}")
    print(candles[-5:])  # Print the last 5 candles
else:
    print("No data found.")
```

#### Fetching and Updating Candles

The main script, `fetch_candles.py`, fetches the latest candle data from the specified exchange and updates the repository. The script is designed to be run automatically via GitHub Actions.

To run the script manually:

```bash
python fetch_candles.py
```

#### GitHub Actions

Tickr uses GitHub Actions to schedule and automate the data fetching process. The workflow is defined in `.github/workflows/update_candles.yml` and is set to run every 15 minutes.

#### Reading Candle Data

You can read the candle data directly from the repository using the provided PyGithub-based client:

```python
from github import Github
import json

# Initialize GitHub client
g = Github("your_github_token")
repo = g.get_repo("your_username/tickr")
file_content = repo.get_contents("data/BTC-USDT_1m.json")

# Load and use the data
candles = json.loads(file_content.decoded_content.decode())
print(candles[-5:])  # Print the last 5 candles
```

### Extending Tickr

To support additional assets or exchanges, you can modify the `symbols` and `exchange` settings in `fetch_candles.py`. The modular design allows easy adaptation to other data sources.

## Contributing

Contributions are welcome! Please fork the repository and create a pull request for any improvements or new features.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.

## Acknowledgments

- [CCXT](https://github.com/ccxt/ccxt) - A cryptocurrency trading library used for fetching market data.
- [PyGithub](https://github.com/PyGithub/PyGithub) - A Python library to interact with the GitHub API.
