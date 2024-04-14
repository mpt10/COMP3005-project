
import os
import psycopg2
import json

season_ids = []
matches_id = []

# Function to connect to the database
def connect_to_database():
    return psycopg2.connect(
        dbname="project_database",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432"
    )

def insert_competition(cursor, data):
    for competition in data:
        if(competition['competition_name'] == "La Liga"):
            if(competition['season_name'] == "2020/2021" or competition['season_name'] == "2019/2020" or competition['season_name'] == "2018/2019"):
                cursor.execute(""" INSERT INTO competition (competition_id, season_id, country_name, competition_name, competition_gender, season_name)
                               VALUES (%s, %s, %s, %s, %s, %s)""", (
                                competition['competition_id'],
                                competition['season_id'],
                                competition['country_name'],
                                competition['competition_name'],
                                competition['competition_gender'],
                                competition['season_name']
                               ))
                season_ids.append(competition['season_id'])
        elif(competition['competition_name'] == "Premier League" and competition['season_name'] == "2003/2004"):
            cursor.execute("""
                               INSERT INTO competition (competition_id, season_id, country_name, competition_name, competition_gender, season_name)
                               VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (season_id) DO NOTHING """, (
                                competition['competition_id'],
                                competition['season_id'],
                                competition['country_name'],
                                competition['competition_name'],
                                competition['competition_gender'],
                                competition['season_name']
                              ))
            season_ids.append(competition['season_id'])

def insert_matches(cursor, data):

    #ok = json.loads(data)
    

    for matches in data:
        if(matches['season']['season_id'] in season_ids):
            home_team_managers_id = None
            away_team_managers_id = None
            stadium_id = None
            referee_id= None 
            competition_stage_id = None
            competition_stage_name = None
            matches_id.append(matches['match_id'])

            if('managers' in matches['home_team']):
                cursor.execute("""INSERT INTO managers (manager_id, manager_name, dob, country_id, country_name)
                            VALUES (%s, %s, %s, %s, %s) ON CONFLICT (manager_id) DO NOTHING """, (
                                matches['home_team']['managers'][0]['id'],
                                matches['home_team']['managers'][0]['name'],
                                matches['home_team']['managers'][0]['dob'],
                                matches['home_team']['managers'][0]['country']['id'],
                                matches['home_team']['managers'][0]['country']['name']
                            ))
                home_team_managers_id = matches['home_team']['managers'][0]['id']

            if('managers' in matches['away_team']):
                cursor.execute("""INSERT INTO managers (manager_id, manager_name, dob, country_id, country_name)
                            VALUES (%s, %s, %s, %s, %s) ON CONFLICT (manager_id) DO NOTHING""", (
                                matches['away_team']['managers'][0]['id'], 
                                matches['away_team']['managers'][0]['name'],
                                matches['away_team']['managers'][0]['dob'],
                                matches['away_team']['managers'][0]['country']['id'],
                                matches['away_team']['managers'][0]['country']['name']
                            ))
                away_team_managers_id = matches['away_team']['managers'][0]['id']
            
            cursor.execute("""INSERT INTO home_team (home_team_id, home_team_name, home_country_id, home_country_name, home_team_gender, home_team_group, manager_id)
                           VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (home_team_id) DO NOTHING""", (
                               matches['home_team']['home_team_id'],
                               matches['home_team']['home_team_name'],
                               matches['home_team']['country']['id'],
                               matches['home_team']['country']['name'],
                               matches['home_team']['home_team_gender'],
                               matches['home_team']['home_team_group'],
                               home_team_managers_id
                           ))
            cursor.execute("""INSERT INTO away_team (away_team_id, away_team_name, away_country_id, away_country_name, away_team_gender, away_team_group, manager_id)
                           VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (away_team_id) DO NOTHING""", (
                               matches['away_team']['away_team_id'],
                               matches['away_team']['away_team_name'],
                               matches['away_team']['country']['id'],
                               matches['away_team']['country']['name'],
                               matches['away_team']['away_team_gender'],
                               matches['away_team']['away_team_group'],
                               away_team_managers_id
                           ))

            if('stadium' in matches):
                cursor.execute("""INSERT INTO stadium (stadium_id, stadium_name, country_id, country_name)
                            VALUES (%s, %s, %s, %s) ON CONFLICT (stadium_id) DO NOTHING""", (
                                matches['stadium']['id'],
                                matches['stadium']['name'],
                                matches['stadium']['country']['id'],
                                matches['stadium']['country']['name']
                            ))
                stadium_id =  matches['stadium']['id']

            if('referee' in matches):
                cursor.execute("""INSERT INTO referee (referee_id, referee_name, referee_country_id, referee_country_name)
                                VALUES (%s, %s, %s, %s) ON CONFLICT (referee_id) DO NOTHING""", (
                                    matches['referee']['id'],
                                    matches['referee']['name'],
                                    matches['referee']['country']['id'],
                                    matches['referee']['country']['name']
                                ))
                referee_id = matches['referee']['id']

            
            if(matches['competition_stage']):
                competition_stage_id = matches['competition_stage']['id']
                competition_stage_name = matches['competition_stage']['name']
            

            cursor.execute("""INSERT INTO matches (match_id, match_date, kick_off, competition_id, season_id, season_name, home_team_id, away_team_id, stadium_id, referee_id, competition_stage_id, competition_stage_name, home_score, away_score, match_status, match_week, last_updated)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",(
                             matches['match_id'],
                             matches['match_date'],
                             matches['kick_off'],
                             matches['competition']['competition_id'],
                             matches['season']['season_id'],
                             matches['season']['season_name'],
                             matches['home_team']['home_team_id'],
                             matches['away_team']['away_team_id'],
                             stadium_id,
                             referee_id,
                             competition_stage_id,
                             competition_stage_name,
                             matches['home_score'],
                             matches['away_score'],
                             matches['match_status'],
                             matches['match_week'],
                             matches['last_updated']
                           ))
    
def insert_player(cursor, lineup):
        for i in range(11):
            cursor.execute("""INSERT INTO player(player_id, player_name, country_id, country_name, jersey_number)
                        VALUES(%s, %s, %s, %s, %s) ON CONFLICT (player_id) DO NOTHING""", (
                                lineup['lineup'][i]['player_id'],
                                lineup['lineup'][i]['player_name'],
                                lineup['lineup'][i]['country']['id'],
                                lineup['lineup'][i]['country']['name'],
                                lineup['lineup'][i]['jersey_number']
                        ))


def insert_lineups(cursor, data):
    for lineup in data:
        insert_player(cursor, lineup)
        cursor.execute("""INSERT INTO team(team_id, team_name, player1_id, player2_id, player3_id, player4_id, player5_id, player6_id, player7_id, player8_id, player9_id, player10_id, player11_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (team_id) DO NOTHING""", (
                        lineup['team_id'],
                        lineup['team_name'],
                        lineup['lineup'][0]['player_id'],
                        lineup['lineup'][1]['player_id'],
                        lineup['lineup'][2]['player_id'],
                        lineup['lineup'][3]['player_id'],
                        lineup['lineup'][4]['player_id'],
                        lineup['lineup'][5]['player_id'],
                        lineup['lineup'][6]['player_id'],
                        lineup['lineup'][7]['player_id'],
                        lineup['lineup'][8]['player_id'],
                        lineup['lineup'][9]['player_id'],
                        lineup['lineup'][10]['player_id'],
                    ))

def insert_types(type_name, cursor, event):
    if(type_name == "Ball Receipt*"):
        if('ball_receipt' in event):
            cursor.execute("""INSERT INTO ball_receipt(type_id, outcome_id, outcome_name)
                        VALUES (%s, %s, %s) """, (
                            event['type']['id'],
                            event['ball_receipt']['outcome']['id'],
                            event['ball_receipt']['outcome']['name'],
                        ))
    if(type_name == "Pass"):
        as_shot_id = None
        shot_as = None
        through_ball = None
        body_part_id = None
        body_part_name = None
        recipient_id = None

        if('assisted_shot_id' in event['pass']):
            as_shot_id = event['pass']['assisted_shot_id']
        
        if('shot_assist' in event['pass']):
            shot_as = event['pass']['shot_assist']
        
        if('through_ball' in event['pass']):
            through_ball = event['pass']['through_ball']

        if('body_part' in event['pass']):
            body_part_id = event['pass']['body_part']['id']
            body_part_name = event['pass']['body_part']['name']

        if('recipient' in event['pass']):
            recipient_id =  event['pass']['recipient']['id']


        cursor.execute("""INSERT INTO pass(type_id, recipient_id, p_length, p_angle, assisted_shot_id, shot_assit, body_part_id, body_part_name, through_ball)
                       VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)""", (
                           event['type']['id'],
                           recipient_id,
                           event['pass']['length'],
                           event['pass']['angle'],
                           as_shot_id,
                           shot_as,
                           body_part_id,
                           body_part_name,
                           through_ball
                       ))

    if(type_name == "Shot"):
        key_pass_id = None
        first_time = None

        if('key_pass_id' in event['shot']):
            key_pass_id = event['shot']['key_pass_id'],
        if('first_time' in event['shot']):
            first_time = event['shot']['first_time']

        cursor.execute("""INSERT INTO shot(type_id, statsbomb_xg, key_pass_id, types_id, first_time, body_part_id, body_part_name)
                       VALUES(%s, %s, %s, %s, %s, %s, %s)""",(
                           event['type']['id'],
                           event['shot']['statsbomb_xg'],
                           key_pass_id,
                           event['shot']['type']['id'],
                           first_time,
                           event['shot']['body_part']['id'],
                           event['shot']['body_part']['name']
                       ))
        
def insert_events(match_id, cursor, data):
    for event in data:
        under_pressure = False
        player_id = None
        dribble_type = None

        if(event['type']):
            insert_types(event['type']['name'], cursor, event)

        if('under_pressure' in event):
            under_pressure = event['under_pressure']

        if('player' in event):
            player_id = event['player']['id']
        
        
        if('dribble' in event):
            dribble_type = event['dribble']['outcome']['name']

        cursor.execute("""INSERT INTO events(event_id, event_index, match_id, type_id, type_name, possession, possession_team_id, team_id, player_id, under_pressure, dribble_type)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", (
                            event['id'],
                            event['index'],
                            match_id,
                            event['type']['id'],
                            event['type']['name'],
                            event['possession'],
                            event['possession_team']['id'],
                            event['team']['id'],
                            player_id,
                            under_pressure,
                            dribble_type
                       ))


# Function to read JSON files from a directory
def read_json_files(directory):
    with open(directory, encoding="utf8") as f:
        return json.load(f)

# Main function
def main():
    conn = connect_to_database()
    cursor = conn.cursor()

    with open(r"C:\Users\meet6\Desktop\open-data\competitions.json", encoding="utf8") as f:
        data = json.load(f)
        insert_competition(cursor, data)

   
    # Traverse through directories
    root_dir = r"C:\Users\meet6\Desktop\open-data\data\matches"
    for dir_name, _, files in os.walk(root_dir):
        for file_name in files:
            if file_name.endswith('.json'):
                file_path = os.path.join(dir_name, file_name)
                data = read_json_files(file_path)
                insert_matches(cursor, data)

    conn.commit()
    

    root_dir = r"C:\Users\meet6\Desktop\open-data\data\lineups"
    for dir_name, _, files in os.walk(root_dir):
        for file_name in files:
            if file_name.endswith('.json'):
                file_path = os.path.join(dir_name, file_name)
                if(int(file_name.split('.')[0]) in matches_id):
                    data = read_json_files(file_path)
                    insert_lineups(cursor, data)
    conn.commit()

    root_dir = r"C:\Users\meet6\Desktop\open-data\data\events"

    for dir_name, _, files in os.walk(root_dir):
        for file_name in files:
            match_id = int(file_name.split('.')[0])
            if(match_id in matches_id):
                print(file_name)
                file_path = os.path.join(dir_name, file_name)
                data = read_json_files(file_path)
                insert_events(match_id, cursor, data)

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
