import os
from Connect.connect import connect
from initialization.create_tables import *
from initialization.add_data import *
from retrieve.retrieve import retrieve_foreign_key


def main():
    conn, curr = connect()
    unique_ids = set()
    create_all_tables(curr)
    add_all_data(curr, unique_ids)
    # retrieve_foreign_key(curr, "agent_id", "agent", "agent_name", "omen")
    # create_tournament_table(curr)
    # add_tournaments(curr, unique_ids)

    # create_agent_table(curr)
    # add_agents(curr, unique_ids)

    # create_player_table(curr)
    # add_players(curr, unique_ids)

    # create_map_table(curr)
    # add_maps(curr, unique_ids)

    # create_match_name_table(curr)
    # add_match_names(curr, unique_ids)

    # create_match_type_table(curr)
    # add_match_types(curr, unique_ids)

    # create_stage_table(curr)
    # add_stages(curr, unique_ids)

    # create_team_table(curr)
    # add_teams(curr, unique_ids)

    # create_draft_phase_table(curr)
    # add_drafts(curr)

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