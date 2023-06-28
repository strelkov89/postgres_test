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
    -- цикл по диапазону идентификаторов облигаций
    FOR bond_id IN 1..200
    LOOP

        -- исключение одной биржи для данной облигации, опредяем случайным образом
        SELECT ARRAY_REMOVE(exchange_list, exchange_list[CEIL(RANDOM() * ARRAY_LENGTH(exchange_list, 1))])
        INTO edited_exchange_list;

        -- цикл по датам, на период 62 дня в прошлое от переданной даты запуска (initial_date)
        FOR i IN 0..62
        LOOP
            trading_date := initial_date - MAKE_INTERVAL(days => i);

            -- переходим к следующей итерации, если дата - выходной (СБ или ВС)
            CONTINUE WHEN is_weekend(trading_date);

            -- цикл по идентификаторам бирж
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