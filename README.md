# Valorant Champions Tour Stats Lab Database

![banner_picture](banner.jpg)


## Table of Contents

- [About](#about)
- [Tables](#tables)
- [Initialization Process](#initialization_process)
- [Getting Started](#getting_started)
- [Usage](#usage)
- [Built With](#built_with)

## About <a name = "about"></a>
This README provides detailed information about the database initialization process, which involves setting up the database schema and inserting the data. The purpose of this code is to ensure that the database is ready for use with the necessary tables and data.

## Tables <a name = "tables"></a>

### Reference Table

#### Purpose

The reference tables for tournaments, stages, match types, matches, teams, players, agents and maps are designed to standardize and organize identifiers and their corresponding IDs. These tables ensure data consistency, maintain relationships, and handle cases where names might be duplicated across different contexts.

#### Hierarchical Structure

The tables are organized hierarchically to reflect the real-world relationships between tournaments, stages, match types, and matches. This hierarchy helps in maintaining data integrity and ensuring that each entry is uniquely identifiable.

#### Table Definitions and Relationships

1. Tournaments
- Columns :
  - tournament_id (Primary key): Unique identifier for each tournament
  - tournament: Name of the tournament
  - year: The year the tournament took place

| tournament_id | tournament                                         | year |   |   |
|---------------|----------------------------------------------------|------|---|---|
| 560           | Champions Tour Asia-Pacific: Last Chance Qualifier | 2021 |   |   |
| 292           | Champions Tour Brazil Stage 1: Challengers 1       | 2021 |   |   |
|               |                                                    |      |   |   |

2. Stages
- Columns :
  - stage_id (Primary key): Unique identifier for each stage
  - tournament_id (Foreign key): References tournament_id from the tournaments table
  - stage: Name of the stage
  - year: The year the stage took place

| stage_id | tournament_id | stage          | year |   |
|----------|---------------|----------------|------|---|
| 2346     | 560           | Main Event     | 2021 |   |
| 594      | 292           | Open Qualifier | 2021 |   |
|          |               |                |      |   |

3. Match Types
- Columns :
  - match_types_id (Primary key): Unique identifier for each match type
  - tournament_id (Foreign key): References tournament_id from the tournaments table 
  - stage_id (Foreign key): References stage_id from the stages table
  - match_type: Name of the match type
  - year: The year the match type took place

| match_type_id | tournament_id | stage_id | match_type    | year |
|---------------|---------------|----------|---------------|------|
| 7577          | 560           | 1096     | Upper Round 1 | 2021 |
| 2674          | 292           | 596      | Opening (A)   | 2021 |
|               |               |          |               |      |

4. Matches
- Columns :
  - match_id (Primary key): Unique identifier for each match
  - tournament_id (Foreign key): References tournament_id from the tournaments table 
  - stage_id (Foreign key): References stage_id from the stages table
  - match_types_id (Foreign key): References match_type_id from the match_types table
  - match: Name of the match
  - year: The year the match took place

| match_id | tournament_id | stage_id | match_type_id | match                     | year |
|----------|---------------|----------|---------------|---------------------------|------|
| 43120    | 560           | 1096     | 7577          | NORTHEPTION vs FULL SENSE | 2021 |
| 9257     | 292           | 596      | 2674          | Vorax vs SLICK            | 2021 |

5. Teams
- Columns :
  - team_id (Primary key): Unique identifier for each team
  - team: Name of the team

| team_id | team            |
|---------|-----------------|
| 198     | Vision Strikers |
| 4050    | FULL SENSE      |

6. Players
- Columns :
  - player_id (Primary key): Unique identifier for each player
  - player: Name of the player

| player_id | player |
|-----------|--------|
| 4462      | MaKo   |
| 485       | stax   |

5. Maps
- Columns :
  - map_id (Primary key): Unique identifier for each map
  - map: Name of the map

| map_id | map   |
|--------|-------|
| 381    | Bind  |
| 498    | Haven |

5. Agents
- Columns :
  - agent_id (Primary key): Unique identifier for each agent
  - agent: Name of the agent

| agent_id | agent  |
|----------|--------|
| 539      | astra  |
| 613      | breach |

#### Usage and Integration

These reference tables are integral for maintaining a normalized and efficient database structure. By linking IDs from related tables, we ensure:

- Data Integrity: Relationships between tournaments, stages, match types, and matches are preserved.

- Consistency: Names and other identifiers are consistently used and easily updated across the database.

- Uniqueness: Even if names are duplicated across different contexts, their unique IDs ensure proper differentiation.


### Main Table

#### Purpose

The main data tables are designed to store core information related to the match, players and their stats, agents and maps. These tables are essential for capturing the primary data needed for the application to function effectively.

### Junction Table


#### Purpose
In the case where a player played multiple agents in a match or a player joined multiple teams in a tournament, a junction table is used to break down a many-to-many relationship into a simpler one-to-many relationship, making it easier to manage and query the data.

#### Structure
The junction table contains a combination of foreign keys and its row index that reference the primary keys of the table. This ensures that each combination of entriesis unique and can be efficiently managed.


## Initialization Process <a name="initialization_process"></a>
1. Create the tables
2. Insert the reference data
3. Check if there are any new data
4. Convert the reference columns to its ID
5. Clean up the data
6. Standarized column names to match the columns in the table
7. Insert the data to the table via memory

## Getting Started <a name = "getting_started"></a>


### Prerequisites

- Python 3.10 or higher
- PostgresSQL
- [VCT Data](https://www.kaggle.com/datasets/ryanluong1/valorant-champion-tour-2021-2023-data)


### Installing


```
$ git clone https://github.com/RyanLuong1/vct-stats-lab-db.git
$ cd vct-stats-lab-db
$ pip3 install -r requirements.txt
```

### Preparation
#### VCT Data
Unzipped the zip files downloaded from Kaggle and move it into the vct-stats-lab-db folder


#### PostgresSQL Database
Follow these [steps](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-postgresql-on-ubuntu-20-04)
from DigitalOcean to install PostgresSQL and setup your database

Then, create an ini file called database.ini and it should look like the following
```
[postgresql]
host=<database_server_ip_address>
database=<database_name>
user=<name_of_the_user>
password=<password_of_the_user>
```


## Usage <a name = "usage"></a>
```
python create_tables.py
python insert_ids.py
python insert_matches_stats.py
python insert_players_stats.py
python insert_agents_stats.py
```

## Built With <a name="built_with"></a>
- [PostgresSQL](https://www.postgresql.org/)
- [Pandas](https://pandas.pydata.org/)
- [asyncio](https://docs.python.org/3/library/asyncio.html)
- [Asyncpg](https://github.com/MagicStack/asyncpg)
- [Psycopg2](https://www.psycopg.org/docs/)