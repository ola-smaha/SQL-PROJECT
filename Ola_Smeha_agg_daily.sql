-- DROP SCHEMA IF EXISTS ola_reporting_schema CASCADE;
-- CREATE SCHEMA IF NOT EXISTS ola_reporting_schema;
DROP TABLE IF EXISTS ola_reporting_schema.agg_daily;
CREATE TABLE IF NOT EXISTS ola_reporting_schema.agg_daily
(
	rental_date DATE,
	late_returns_perc NUMERIC,
	total_store1_rentals INT,
	total_store2_rentals INT,
	total_store1_customers INT,
	total_store2_customers INT,
	active_cust_store1_perc NUMERIC,
	active_cust_store2_perc NUMERIC,
	total_india_customers INT,
	total_united_kingdom_customers INT,
	total_sports_rentals INT,
	total_animation_rentals INT,
	total_travel_rentals INT,
	total_music_rentals INT,
	total_pg13_rentals INT,
	total_nc17_rentals INT,
	total_g_rentals INT,
	total_pg_rentals INT,
	total_r_rentals INT
);
---------------------------------------------------------------------------------
-- PERCENTAGE OF LATE RETURNS
WITH CTE_RENTAL_DURATION_DETAILS AS
(
	SELECT
		CAST(r.rental_date AS date) AS rental_day,
		COUNT (
			CASE
				WHEN EXTRACT(DAY FROM (r.return_date - r.rental_date))
					+ EXTRACT(HOUR FROM (r.return_date - r.rental_date))/24 > f.rental_duration
				THEN r.rental_id
			END
		) AS total_above_allowed_rental_duration_rentals,
		COUNT(DISTINCT rental_id) AS total_rentals
	FROM public.rental r
	INNER JOIN inventory i
	ON i.inventory_id = r.inventory_id
	INNER JOIN public.film f
	ON f.film_id = i.film_id
	GROUP BY
		CAST(r.rental_date AS date)
),
CTE_LATE_RETURNS_PERC AS
(
	SELECT
		rental_day,
		ROUND(CAST(total_above_allowed_rental_duration_rentals AS NUMERIC)/ NULLIF(CAST(total_rentals AS NUMERIC),0)*100 ,2) AS percentage
	FROM CTE_RENTAL_DURATION_DETAILS
),
---------------------------------------------------------------------------------
-- TOTAL RENTALS OF STORES 1&2
CTE_STORES_TOTAL_RENTALS AS
(
	SELECT
		CAST(se_rental.rental_date AS DATE) AS rental_day,
		COUNT(DISTINCT
			CASE
				WHEN se_inventory.store_id = 1
				THEN se_rental.rental_id
			END
		) AS total_store1_rentals,
		COUNT(DISTINCT
		CASE
			WHEN se_inventory.store_id = 2
			THEN se_rental.rental_id
		END
		) AS total_store2_rentals
	FROM public.rental se_rental
	LEFT JOIN inventory se_inventory
		ON se_rental.inventory_id = se_inventory.inventory_id
	GROUP BY
		CAST(se_rental.rental_date AS DATE)
),
---------------------------------------------------------------------------------
-- TOTAL CUSTOMERS OF STORES 1&2
CTE_TOTAL_CUSTOMERS AS
(
	SELECT
		CAST(se_rental.rental_date AS DATE) AS rental_day,
		COUNT(DISTINCT
			CASE
				WHEN se_customer.store_id = 1
				THEN se_customer.customer_id
			END
		) AS total_store1_customers,
		COUNT(DISTINCT
		CASE
			WHEN se_customer.store_id = 2
			THEN se_customer.customer_id
		END
		) AS total_store2_customers
	FROM public.customer se_customer
	LEFT OUTER JOIN public.rental se_rental
		ON se_customer.customer_id = se_rental.customer_id
	GROUP BY
		CAST(se_rental.rental_date AS DATE)
),
---------------------------------------------------------------------------------
-- TOTAL ACTIVE CUSTOMERS WHO MADE RENTALS AND ARE MARKED AS ACTIVE CUSTOMERS OF STORES 1&2
CTE_ACTIVE_CUSTOMERS AS
(
	SELECT
		CAST(se_rental.rental_date AS DATE) AS rental_day,
		COUNT(DISTINCT
			CASE
				WHEN se_customer.store_id = 1
					AND se_customer.active = 1
				THEN se_customer.customer_id
			END
		) AS total_store1_active_customers,
		COUNT(DISTINCT
		CASE
			WHEN se_customer.store_id = 2
				AND se_customer.active = 1
			THEN se_customer.customer_id
		END
		) AS total_store2_active_customers
	FROM public.customer se_customer
	LEFT OUTER JOIN public.rental se_rental
		ON se_customer.customer_id = se_rental.customer_id
	GROUP BY
		CAST(se_rental.rental_date AS DATE)
),

-- PERCENTAGE OF ACTIVE CUSTOMERS IN STORES 1&2
CTE_PERCENTAGE_ACTIVE_CUST AS
(
	SELECT
		CTE_TOTAL_CUSTOMERS.rental_day,
		ROUND(CAST(CTE_ACTIVE_CUSTOMERS.total_store1_active_customers AS NUMERIC)
		/ NULLIF(CAST(CTE_TOTAL_CUSTOMERS.total_store1_customers AS NUMERIC),0) *100,2) AS active_cust_store1_perc,
		ROUND(CAST(CTE_ACTIVE_CUSTOMERS.total_store2_active_customers AS NUMERIC)
		/ NULLIF(CAST(CTE_TOTAL_CUSTOMERS.total_store2_customers AS NUMERIC),0) *100,2) AS active_cust_store2_perc
	FROM CTE_TOTAL_CUSTOMERS 
	INNER JOIN CTE_ACTIVE_CUSTOMERS
		ON CTE_TOTAL_CUSTOMERS.rental_day = CTE_ACTIVE_CUSTOMERS.rental_day
	GROUP BY
		CTE_TOTAL_CUSTOMERS.rental_day,
		ROUND(CAST(CTE_ACTIVE_CUSTOMERS.total_store1_active_customers AS NUMERIC)
		/ NULLIF(CAST(CTE_TOTAL_CUSTOMERS.total_store1_customers AS NUMERIC),0) *100,2),
		ROUND(CAST(CTE_ACTIVE_CUSTOMERS.total_store2_active_customers AS NUMERIC)
		/ NULLIF(CAST(CTE_TOTAL_CUSTOMERS.total_store2_customers AS NUMERIC),0) *100,2)
),
---------------------------------------------------------------------------------
-- TOTAL CUSTOMERS FROM COUNTRIES WHERE MOST AND LEAST RENTALS WERE MADE
CTE_TOTAL_CUSTOMERS_PER_TOP3_COUNTRIES AS
(
	SELECT
		CAST(se_rental.rental_date AS DATE) AS rental_day,
		COUNT(
			CASE
				WHEN se_country.country = 'India'
				THEN se_rental.customer_id
			END
		) AS total_india_customers,
		COUNT(
			CASE
				WHEN se_country.country = 'United Kingdom'
				THEN se_rental.customer_id
			END
		) AS total_united_kingdom_customers
	FROM public.rental se_rental
	INNER JOIN public.customer se_customer
		ON se_customer.customer_id = se_rental.customer_id
	INNER JOIN public.address se_address
		ON se_address.address_id = se_customer.address_id
	INNER JOIN public.city se_city
		ON se_city.city_id = se_address.city_id
	INNER JOIN public.country se_country
		ON se_country.country_id = se_city.country_id
	GROUP BY
		CAST(se_rental.rental_date AS DATE)
),
---------------------------------------------------------------------------------
-- TOTAL RENTALS OF TOP 2 CATEGORIES: SPORTS, ANIMATION
-- AND LEAST 2 RENTED CATEGORIES: TRAVEL, MUSIC
CTE_TOTAL_TOP_LEAST_CATEGORIES_RENTALS AS
(
	SELECT
		CAST(se_rental.rental_date AS DATE) AS rental_day,
		COUNT(
			CASE
				WHEN se_film_category.category_id = 15
				THEN se_rental.rental_id
			END
			) AS total_sports_rentals,
		COUNT(
			CASE
				WHEN se_film_category.category_id = 2
				THEN se_rental.rental_id
			END
			) AS total_animation_rentals,
		COUNT(
			CASE
				WHEN se_film_category.category_id = 16
				THEN se_rental.rental_id
			END
			) AS total_travel_rentals,
		COUNT(
			CASE
				WHEN se_film_category.category_id = 12
				THEN se_rental.rental_id
			END
			) AS total_music_rentals
	FROM public.rental se_rental
	LEFT JOIN public.inventory se_inventory
		ON se_rental.inventory_id = se_inventory.inventory_id
	LEFT JOIN public.film_category se_film_category
		ON se_film_category.film_id = se_inventory.film_id
	GROUP BY
		CAST(se_rental.rental_date AS DATE)
),
---------------------------------------------------------------------------------
-- TOTAL RENTALS IN EACH RATING
CTE_TOTAL_RENTALS_PER_RATING AS
(
	SELECT
		CAST(se_rental.rental_date AS DATE) AS rental_day,
		COUNT(
			CASE
				WHEN se_film.rating = 'PG-13'
				THEN se_rental.rental_id
			END
			) AS total_pg13_rentals,
		COUNT(
			CASE
				WHEN se_film.rating = 'NC-17'
				THEN se_rental.rental_id
			END
			) AS total_nc17_rentals,
		COUNT(
			CASE
				WHEN se_film.rating = 'G'
				THEN se_rental.rental_id
			END
			) AS total_g_rentals,
		COUNT(
			CASE
				WHEN se_film.rating = 'PG'
				THEN se_rental.rental_id
			END
			) AS total_pg_rentals,
		COUNT(
			CASE
				WHEN se_film.rating = 'R'
				THEN se_rental.rental_id
			END
			) AS total_r_rentals
	FROM public.rental se_rental
	LEFT JOIN public.inventory se_inventory
		ON se_inventory.inventory_id = se_rental.inventory_id
	LEFT JOIN public.film se_film
		ON se_film.film_id = se_inventory.film_id
	GROUP BY 
		CAST(se_rental.rental_date AS DATE)
),
-----------------------------------------------------------------------------------------------------------------------------------
CTE_DAILY_ACTIVITY AS
(
	SELECT
		late_returns.rental_day,
		late_returns.percentage,
		store_rentals.total_store1_rentals,
		store_rentals.total_store2_rentals,
		total_customers.total_store1_customers,
		total_customers.total_store2_customers,
		perc_active_customers.active_cust_store1_perc,
		perc_active_customers.active_cust_store2_perc,
		customers_per_country.total_india_customers,
		customers_per_country.total_united_kingdom_customers,
		rentals_per_category.total_sports_rentals,
		rentals_per_category.total_animation_rentals,
		rentals_per_category.total_travel_rentals,
		rentals_per_category.total_music_rentals,
		rentals_per_rating.total_pg13_rentals,
		rentals_per_rating.total_nc17_rentals,
		rentals_per_rating.total_g_rentals,
		rentals_per_rating.total_pg_rentals,
		rentals_per_rating.total_r_rentals 
	FROM CTE_LATE_RETURNS_PERC AS late_returns
	LEFT JOIN CTE_STORES_TOTAL_RENTALS AS store_rentals
		ON late_returns.rental_day = store_rentals.rental_day
	LEFT JOIN CTE_TOTAL_CUSTOMERS AS total_customers
		ON late_returns.rental_day = total_customers.rental_day
	LEFT JOIN CTE_PERCENTAGE_ACTIVE_CUST AS perc_active_customers
		ON late_returns.rental_day = perc_active_customers.rental_day
	LEFT JOIN CTE_TOTAL_CUSTOMERS_PER_TOP3_COUNTRIES AS customers_per_country
		ON late_returns.rental_day = customers_per_country.rental_day
	LEFT JOIN CTE_TOTAL_TOP_LEAST_CATEGORIES_RENTALS AS rentals_per_category
		ON late_returns.rental_day = rentals_per_category.rental_day
	LEFT JOIN CTE_TOTAL_RENTALS_PER_RATING AS rentals_per_rating
		ON late_returns.rental_day = rentals_per_rating.rental_day
	ORDER BY
		late_returns.rental_day
)

INSERT INTO ola_reporting_schema.agg_daily
(
	SELECT * FROM CTE_DAILY_ACTIVITY
);

SELECT * FROM ola_reporting_schema.agg_daily;

----------------------------------------------------------------------------------------------
-- QUERIES USED IN PYTHON

-- Percentage of late returns:
SELECT
	daily_agg.rental_date,
	daily_agg.late_returns_perc
FROM ola_reporting_schema.agg_daily AS daily_agg

-- Average Rentals per Store
SELECT
	AVG(daily_agg.total_store1_rentals) AS total_store1_rentals,
	AVG(daily_agg.total_store2_rentals) AS total_store2_rentals
FROM ola_reporting_schema.agg_daily AS daily_agg

-- Total Customers per Store on Each Rental Day
SELECT
	daily_agg.rental_date,
	daily_agg.total_store1_customers,
	daily_agg.total_store2_customers
FROM ola_reporting_schema.agg_daily AS daily_agg

-- Customer Behavior from the Two Countries India and UK
SELECT
	daily_agg.rental_date,
	daily_agg.total_india_customers,
	daily_agg.total_united_kingdom_customers
FROM ola_reporting_schema.agg_daily AS daily_agg

-- Total Rentals of the Two Most and Least Rented Categories
SELECT
	SUM(daily_agg.total_sports_rentals) AS total_sports_rentals,
	SUM(daily_agg.total_animation_rentals) AS total_animation_rentals,
	SUM(daily_agg.total_travel_rentals) AS total_travel_rentals,
	SUM(daily_agg.total_music_rentals) AS total_music_rentals
FROM ola_reporting_schema.agg_daily AS daily_agg

-------------------------------------------------------------------------------
-- Views used:
-- 15 COUNTRIES FROM WHICH CUSTOMERS WHO MADE RENTALS LIVE
CREATE VIEW ola_reporting_schema.view_customers_who_rented_15countries AS
(
SELECT
	country.country,
	COUNT(DISTINCT rental.customer_id) AS total_customers
FROM public.rental rental
INNER JOIN public.customer cust
	ON cust.customer_id = rental.customer_id
INNER JOIN public.address address
	ON address.address_id = cust.address_id
INNER JOIN public.city city
	ON city.city_id = address.city_id
INNER JOIN public.country country
	ON country.country_id = city.country_id
GROUP BY
	country.country
ORDER BY
	COUNT(DISTINCT rental.customer_id) DESC
LIMIT 15
);

-- STORE LOCATIONS
CREATE VIEW ola_reporting_schema.view_store_locations AS
(
SELECT
	store.store_id,
	country.country
FROM public.store store
INNER JOIN public.address address
	ON store.address_id = address.address_id
INNER JOIN public.city city
	ON city.city_id = address.city_id
INNER JOIN public.country country
	ON country.country_id = city.country_id
);
-------------------------------------------------------------------------------
-- Demographic Analysis
SELECT *
FROM ola_reporting_schema.view_customers_who_rented_15countries

SELECT *
FROM ola_reporting_schema.view_store_locations

-- Rating Analysis
SELECT
	SUM(daily_agg.total_pg13_rentals) AS total_pg13_rentals,
	SUM(daily_agg.total_nc17_rentals) AS total_nc17_rentals,
	SUM(daily_agg.total_g_rentals) AS total_g_rentals,
	SUM(daily_agg.total_pg_rentals) AS total_pg_rentals,
	SUM(daily_agg.total_r_rentals) AS total_r_rentals
FROM ola_reporting_schema.agg_daily AS daily_agg


