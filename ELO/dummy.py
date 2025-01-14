import csv
import random

# Generate dummy games.csv
def generate_games_file(filename, num_games, num_players):
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Game_ID", "Winner_Team", "Loser_Team"])
        for game_id in range(1, num_games + 1):
            # Randomly select 2 players for the winning team and 2 for the losing team
            players = random.sample(range(1, num_players + 1), 4)
            winner_team = f"player{players[0]};player{players[1]}"
            loser_team = f"player{players[2]};player{players[3]}"
            writer.writerow([game_id, winner_team, loser_team])

# Generate files
generate_games_file("games.csv", 1000, 100)  # 1000 games with 100 players

print("Dummy files generated: ratings.csv and games.csv")
