class SqlQueries:
    immigrant_table_insert = ("""
        SELECT
            immigrant_id
            , arrival_port as port
            , state_of_residence as state
            , arrival_date
        FROM staging_immigration
        WHERE immigrant_id IS NOT NULL
    """)

    immigrant_stats_table_insert = ("""
        SELECT
            immigrant_id
            , immigration_id
            , gender
            , immigrant_age as age
            , origin_country
            , visa_type
            , visitor_type
        FROM staging_immigration
        WHERE immigrant_id IS NOT NULL
    """)

    city_population_table_insert = ("""
        SELECT
            x.city
            , x.port
            , x.state
            , x.male_population
            , x.female_population
            , x.total_population
            , x.foreign_born
        FROM staging_demographics x
        -- only for cities that are a destination
        -- for immigrations
        JOIN staging_immigration y
            ON x.port = y.arrival_port
    """)

    city_demographics_table_insert = ("""
        SELECT
            x.city
            , x.port
            , x.state
            , x.race
            , x.n_persons
        FROM staging_demographics x
        -- only for cities that are a destination
        -- for immigrations
        JOIN staging_immigration y
            ON x.port = y.arrival_port
    """)

    arrival_info_table_insert = ("""
        SELECT
            immigrant_id
            , arrival_port as port
            , port_type
            , airline_code
            , airline_flight_number
        FROM staging_immigration
        WHERE immigrant_id IS NOT NULL
    """)

    arrival_date_table_insert = ("""
        SELECT
            arrival_date
            , immigration_year
            , immigration_month
        FROM staging_immigration
        WHERE immigrant_id IS NOT NULL
            AND arrival_date IS NOT NULL
    """)