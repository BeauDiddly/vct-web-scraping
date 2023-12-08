import os
from Connect.connect import connect
from initialization.initialization import *


def main():
    conn, curr = connect()
    create_tournament_table(curr)
    add_tournaments(curr)

    create_agent_table(curr)
    add_agents(curr)

    create_player_table(curr)
    add_players(curr)

    create_map_table(curr)
    add_maps(curr)

    create_match_name_table(curr)
    add_match_names(curr)

    create_match_type_table(curr)
    add_match_types(curr)

    create_stage_table(curr)
    add_stages(curr)

    create_team_table(curr)
    add_teams(curr)

    conn.commit()
    curr.close()
    conn.close()


# setup_path = os.path.join(root, "vct_stats_lab_db/setup")

# from setup.connect import connect

# root = os.path.dirname(os.path.abspath(__file__))
# setup_path = os.path.join(root, "vct_stats_lab_db/setup")
# sys.path.append("/vct_stats_lab_db/setup")
# from setup.connect import connect

if __name__ == '__main__':
    main()