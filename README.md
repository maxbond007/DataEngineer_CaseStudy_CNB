# CNB Exchange Rates ETL Script

## Overview
This script automates the extraction and transformation of exchange rates from the **Czech National Bank (ČNB)**:
1. **Daily Exchange Rates**: Fetched for the current date.
2. **Monthly Additional Exchange Rates**: Fetched for the current month.

The combined data is saved as a **Parquet file**.

---

## Requirements
- **Python 3.8+**
- Install dependencies:
  ```bash
  pip install requests pandas pyarrow
