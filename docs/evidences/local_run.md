# Evidencia de execucao local

Data da validacao: 2026-04-17

Comando equivalente executado localmente:

```powershell
$env:PYTHONPATH=".\src"
python -m pipeline.seed_orders --db-path data/raw/orders.db --reset
python -m pipeline.extract_orders --db-path data/raw/orders.db --output-path data/staging/orders_raw_window_2026-03-16.csv --process-date 2026-03-16 --lookback-days 2
python -m pipeline.processing --input-path data/staging/orders_raw_window_2026-03-16.csv --csv-output-path data/curated/orders_daily_summary.csv --mart-db-path data/curated/orders_mart.db --process-date 2026-03-16 --lookback-days 2
```

Resultado observado em `data/curated/orders_daily_summary.csv`:

```text
business_date,status,orders_count,customers_count,total_amount,max_ingested_at,processed_for_date,processed_at
2026-03-14,paid,1,1,150.0,2026-03-16 09:00:00,2026-03-16,<timestamp da execucao>
2026-03-15,paid,3,3,400.0,2026-03-16 09:05:00,2026-03-16,<timestamp da execucao>
```

Validacao de testes:

```text
2 passed, 2 skipped in 0.34s
```

Os dois testes ignorados por padrao sao testes de integracao Spark no Windows. Eles podem ser habilitados com `RUN_SPARK_TESTS=1` em ambientes onde o PySpark local esteja estavel, preferencialmente Linux, WSL ou Docker.

Observacao: no Windows, o PySpark pode emitir avisos sobre `winutils.exe` ou limpeza de diretorios temporarios. Nesta validacao eles nao impediram a execucao, que terminou com codigo de saida `0` e gerou as saidas esperadas.
