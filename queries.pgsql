-- Все решения выполнены в PostgreSQL 15.  
-- Задание 1. Решение.
-- Создание таблицы exch_quotes_archive
CREATE TABLE IF NOT EXISTS exch_quotes_archive
(
    exchange_id  INTEGER NOT NULL,
    bond_id      INTEGER NOT NULL,
    trading_date DATE    NOT NULL,
    bid          NUMERIC(10, 2),
    ask          NUMERIC(10, 2),
    PRIMARY KEY (exchange_id, bond_id, trading_date)
);

-- Ф-я для определения является ли input_date выходным днём
CREATE OR REPLACE FUNCTION is_weekend(input_date DATE)
    RETURNS BOOLEAN AS
$BODY$
DECLARE
    day_of_week INT;
BEGIN
    day_of_week := EXTRACT(DOW FROM input_date);
    IF day_of_week = 0 OR day_of_week = 6 THEN
        RETURN TRUE;
    ELSE
        RETURN FALSE;
    END IF;
END;
$BODY$
    LANGUAGE plpgsql;

-- Ф-я для заполнения таблицы exch_quotes_archive
CREATE OR REPLACE FUNCTION fill_exch_quotes_archive(
    initial_date DATE DEFAULT CURRENT_DATE
)
    RETURNS VOID AS
$BODY$
DECLARE
    exchange_id          INT;
    trading_date         DATE;
    exchange_list        INT[] := ARRAY [1,4,72,99,250,399,502,600];
    edited_exchange_list INT[];
    random_bid           NUMERIC(10, 2);
    random_ask           NUMERIC(10, 2);
BEGIN
    -- Цикл по диапазону идентификаторов облигаций
    FOR bond_id IN 1..200
    LOOP

        -- Исключение одной биржи для данной облигации, опредяем случайным образом
        SELECT ARRAY_REMOVE(exchange_list, exchange_list[CEIL(RANDOM() * ARRAY_LENGTH(exchange_list, 1))])
        INTO edited_exchange_list;

        -- Цикл по датам, на период 62 дня в прошлое от переданной даты запуска (initial_date)
        FOR i IN 0..62
        LOOP
            trading_date := initial_date - MAKE_INTERVAL(days => i);

            -- Переходим к следующей итерации, если дата - выходной (СБ или ВС)
            CONTINUE WHEN is_weekend(trading_date);

            -- Цикл по идентификаторам бирж
            FOREACH exchange_id IN ARRAY edited_exchange_list
            LOOP
                -- Генерируем случайные значения для bid и ask
                IF RANDOM() < 0.8
                THEN
                    random_bid := RANDOM() * 2 - 0.01;
                    random_ask := RANDOM() * 2 - 0.01;
                ELSE
                    random_bid := RANDOM() * 3;
                    random_ask := RANDOM() * 3;
                END IF;

                -- Определяем, какое поле будет NULL
                IF RANDOM() < 0.1 THEN
                    IF RANDOM() < 0.5 THEN
                        random_bid := NULL;
                    ELSE
                        random_ask := NULL;
                    END IF;
                END IF;

                INSERT INTO exch_quotes_archive (exchange_id, bond_id, trading_date, bid, ask)
                VALUES (exchange_id, bond_id, trading_date, random_bid, random_ask);
            END LOOP;
        END LOOP;
    END LOOP;
END;
$BODY$
    LANGUAGE plpgsql;

TRUNCATE exch_quotes_archive;
SELECT fill_exch_quotes_archive('2023-06-28');


-- Задание 2. Решение.
-- full_list - полный список дат, включая выходные дни
WITH full_list (trading_date, bond_id) AS (
    WITH dates_list (trading_date) AS (
            SELECT i::DATE
            FROM GENERATE_SERIES('2023-06-20'::DATE - INTERVAL '13 days', '2023-06-20'::DATE, '1 day'::INTERVAL) i
        ),
        bonds_list (bond_id) AS (SELECT DISTINCT bond_id FROM exch_quotes_archive)
    SELECT * FROM dates_list CROSS JOIN bonds_list
    ),
    -- bond_avg_prices - выборка средних цен (и bid, и ask) от всех бирж по каждой облигации
    bond_avg_prices AS (
        SELECT bond_id, trading_date, ROUND(AVG(bid), 2) AS avg_bid, ROUND(AVG(ask), 2) AS avg_ask
        FROM exch_quotes_archive
        WHERE trading_date >= '2023-06-20'::DATE - INTERVAL '14 days'
        GROUP BY bond_id, trading_date
    )
SELECT full_list.trading_date, full_list.bond_id, avg_bid, avg_ask
FROM full_list
LEFT JOIN bond_avg_prices ON (
    full_list.trading_date = bond_avg_prices.trading_date
    AND full_list.bond_id = bond_avg_prices.bond_id
)
ORDER BY full_list.trading_date, full_list.bond_id;


-- Задание 3. Решение.
-- full_list - полный список дат, включая выходные дни
WITH full_list (trading_date, bond_id) AS (
    WITH dates_list (trading_date) AS (
        SELECT i::DATE
        FROM GENERATE_SERIES('2023-06-20'::DATE - INTERVAL '13 days', '2023-06-20'::DATE, '1 day'::INTERVAL) i
    ),
        bonds_list (bond_id) AS (SELECT DISTINCT bond_id FROM exch_quotes_archive)
    SELECT * FROM dates_list CROSS JOIN bonds_list
    ),
    -- bond_avg_prices - выборка средних цен (и bid, и ask) от всех бирж по каждой облигации
    bond_avg_prices AS (
        WITH recursive_bond_avg_prices AS (
            SELECT bond_id, trading_date, ROUND(AVG(bid), 2) AS avg_bid, ROUND(AVG(ask), 2) AS avg_ask
            FROM exch_quotes_archive
            WHERE trading_date >= '2023-06-20'::DATE - INTERVAL '14 days'
            GROUP BY bond_id, trading_date
        )
        -- Вычисление скользящего среднего, с окном равным 3.
        -- Вычисление производим с использованием оконной функции
        SELECT *, ROUND(AVG(avg_bid) OVER w, 2) AS roll_avg_bid, ROUND(AVG(avg_ask) OVER w, 2) AS roll_avg_ask
        FROM recursive_bond_avg_prices
        WINDOW w AS (
            ORDER BY bond_id, trading_date
            ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
        )
    )
SELECT full_list.trading_date, full_list.bond_id, avg_bid, avg_ask, roll_avg_bid, roll_avg_ask
FROM full_list
LEFT JOIN bond_avg_prices ON (
    full_list.trading_date = bond_avg_prices.trading_date
    AND full_list.bond_id = bond_avg_prices.bond_id
)
ORDER BY full_list.bond_id, full_list.trading_date;