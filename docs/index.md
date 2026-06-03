# BagelQuant Data

`bagelquant-data` is the provider-neutral data layer for BagelQuant.

It guarantees consistent, reliable, reproducible access to data. It does not
own research, portfolio construction, graph execution, backtesting, or
analytics.

The package also ships an optional Streamlit GUI for managing a local data lake:

```bash
uv sync --extra gui --extra tushare
uv run streamlit run src/bagelquant_data/gui/app.py
```
