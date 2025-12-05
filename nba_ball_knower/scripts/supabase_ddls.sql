-- Dimension: Teams
drop table public.dim_teams;
create table public.dim_teams (
    team_id             integer primary key,
    full_name           text,
    abbreviation        text,
    nickname            text,
    city                text,
    state               text,
    year_founded        integer,
    date_time_processed timestamptz not null
);


-- Dimension: Players
create table if not exists public.dim_players (
    player_id           integer primary key,
    first_name          text,
    last_name           text,
    full_name           text,
    is_active           boolean,
    team_id             integer,
    date_time_processed timestamptz not null,

    constraint dim_players_team_fk
        foreign key (team_id) references dim_teams(team_id)
);


-- Fact: team games
create table if not exists public.fact_team_games (
    season_id           text,
    team_id             integer,
    team_abbreviation   text,
    team_name           text,
    game_id             text,
    game_date           date,
    matchup             text,
    wl                  text,
    min                 numeric,
    pts                 numeric,
    fgm                 numeric,
    fga                 numeric,
    fg_pct              numeric,
    fg3m                numeric,
    fg3a                numeric,
    fg3_pct             numeric,
    ftm                 numeric,
    fta                 numeric,
    ft_pct              numeric,
    oreb                numeric,
    dreb                numeric,
    reb                 numeric,
    ast                 numeric,
    stl                 numeric,
    blk                 numeric,
    tov                 numeric,
    pf                  numeric,
    plus_minus          numeric,
    date_time_processed timestamptz not null,

    constraint fact_team_games_pk
        primary key (game_id, team_id),

    constraint fact_team_games_team_fk
        foreign key (team_id) references dim_teams(team_id)
);


-- create player season stats 
create table if not exists public.fact_player_season_stats (
    player_id               integer,
    player_name             text,
    nickname                text,
    team_id                 integer,
    team_abbreviation       text,
    age                     numeric,
    gp                      integer,
    w                       integer,
    l                       integer,
    w_pct                   numeric,
    min                     numeric,
    fgm                     numeric,
    fga                     numeric,
    fg_pct                  numeric,
    fg3m                    numeric,
    fg3a                    numeric,
    fg3_pct                 numeric,
    ftm                     numeric,
    fta                     numeric,
    ft_pct                  numeric,
    oreb                    numeric,
    dreb                    numeric,
    reb                     numeric,
    ast                     numeric,
    tov                     numeric,
    stl                     numeric,
    blk                     numeric,
    blka                    numeric,
    pf                      numeric,
    pfd                     numeric,
    pts                     numeric,
    plus_minus              numeric,
    nba_fantasy_pts         numeric,
    dd2                     integer,
    td3                     integer,
    wnba_fantasy_pts        numeric,
    gp_rank                 integer,
    w_rank                  integer,
    l_rank                  integer,
    w_pct_rank              integer,
    min_rank                integer,
    fgm_rank                integer,
    fga_rank                integer,
    fg_pct_rank             integer,
    fg3m_rank               integer,
    fg3a_rank               integer,
    fg3_pct_rank            integer,
    ftm_rank                integer,
    fta_rank                integer,
    ft_pct_rank             integer,
    oreb_rank               integer,
    dreb_rank               integer,
    reb_rank                integer,
    ast_rank                integer,
    tov_rank                integer,
    stl_rank                integer,
    blk_rank                integer,
    blka_rank               integer,
    pf_rank                 integer,
    pfd_rank                integer,
    pts_rank                integer,
    plus_minus_rank         integer,
    nba_fantasy_pts_rank    integer,
    dd2_rank                integer,
    td3_rank                integer,
    wnba_fantasy_pts_rank   integer,
    team_count              integer,
    season                  text,
    date_time_processed     timestamptz not null,

    constraint fact_player_season_stats_pk
        primary key (player_id, season),

    constraint fact_player_season_stats_team_fk
        foreign key (team_id) references dim_teams(team_id),

    constraint fact_player_season_stats_player_fk
        foreign key (player_id) references dim_players(player_id)
);
