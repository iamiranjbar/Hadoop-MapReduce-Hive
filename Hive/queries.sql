SELECT symbol, close_price 
FROM default.stock_exchange_daily
WHERE symbol_date == "1400-01-26"
ORDER BY close_price DESC
LIMIT 10;


SELECT symbol, close_price 
FROM default.stock_exchange_daily 
WHERE symbol_date == "1400-01-26"
ORDER BY close_price ASC 
LIMIT 10;


SELECT symbol, volume
FROM default.stock_exchange_daily
ORDER BY volume DESC 
LIMIT 1;

SELECT *
FROM (
	SELECT symbol, symbol_month, month_change,
	row_number() over (partition by symbol_month order by month_change desc) as symbol_rank
	FROM (
		SELECT symbol, MONTH(symbol_date) as symbol_month, SUM(close_price_change) as month_change
		FROM default.stock_exchange_daily 
		GROUP BY symbol, MONTH(symbol_date)
		) AS symbol_month_change
	) AS ranked_symbols
WHERE symbol_rank <= 10;

SELECT symbol, SUM(close_price_change) as period_change
FROM default.stock_exchange_daily 
GROUP BY symbol
ORDER BY period_change ASC
LIMIT 10;

SELECT symbol, COUNT(*) as cnt
FROM default.stock_exchange_daily 
GROUP BY symbol
ORDER BY cnt ASC
LIMIT 100;
