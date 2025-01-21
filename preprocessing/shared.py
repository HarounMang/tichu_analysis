def split_dealing_turn(head: str):
    gr_cards_lines_other_split = head.split("---------------Startkarten------------------\n", 1)
    gr_cards_lines_split = gr_cards_lines_other_split[0].split("\n")
    start_cards_lines_other_split = gr_cards_lines_other_split[1].split("Schupfen:\n", 1)
    start_cards_lines_split = start_cards_lines_other_split[0].split("\n")[:-1]  # [:-1] to get rid of the empty string
    deal_lines_split = start_cards_lines_other_split[1].split("\n")[:-1]  # [:-1] to get rid of the empty string

    bombs_player_id = []
    if "BOMBE" in deal_lines_split[-1]:
        bombs_players = deal_lines_split[-1].split(":")[1].strip().split(" ")
        bombs_player_id = [int(player[1]) for player in bombs_players]
        deal_lines_split = deal_lines_split[:4]

    return gr_cards_lines_split, start_cards_lines_split, deal_lines_split, bombs_player_id


def starting_turn_information(gr_cards_line: str, start_cards_line: str, deal_line: str, hands: list[set[str]],
                              player_id: int) -> list[list[str]]:
    information = {
        "name": None,
        "gr_cards": set(),
        "extra_cards": set(),
        "deal": {
            "left": None,
            "middle": None,
            "right": None,
        }
    }

    player_gr_cards_split = gr_cards_line.split(" ", 1)
    name = player_gr_cards_split[0][3:]

    information["name"] = name
    gr_cards = set()

    for card in player_gr_cards_split[1].split(" ")[:-1]:
        gr_cards.add(card)
        hands[player_id].add(card)

    extra_cards = set()

    for card in start_cards_line.split(" ", 1)[1].split(" ")[:-1]:
        if card not in gr_cards:
            extra_cards.add(card)
            hands[player_id].add(card)

    left, middle, right = None, None, None

    for deal_line in deal_line.split("gibt: ", 1)[1:]:
        deal_line_split = deal_line.split(": ", 3)[1:]
        left = deal_line_split[1].split(" - ", maxsplit=1)[0]
        middle = deal_line_split[2].split(" - ", maxsplit=1)[0]
        
        for i, deal_string in enumerate(deal_line.split(": ", 3)[1:], start=1):
            card = deal_string.split(" - ", maxsplit=1)[0]
            
            if i == 1:
                left = card
            elif i == 2:
                middle = card
            elif i == 3:
                right = card

            hands[player_id].remove(card)
            hands[(player_id + i) % 4].add(card)

    return name, gr_cards, extra_cards, {"left": left, "middle": middle, "right": right}


def get_gr_tichu_callers(start_cards_lines_split: list[str]) -> set[int]:
    # from the example of the head you can see the grand tichu calls are after the 4 'startkarten' rows
    gr_tichu_called = len(start_cards_lines_split) > 4  # because there are 4 lines + possible Grand Tichu callers
    gr_tichu_callers: set[int] = set()
    return set(gr_tichu_callers.add(int(line[16])) for line in start_cards_lines_split[4:] if gr_tichu_called)


def other_turns_information(turns: list[str], hands: list[set[str]]):
    rows: list[list[any]] = [[], [], [], []]
    tichu_callers: set[int] = set()
    finished_position = 1.0 
    finished_players: set[int] = set()
    game_ended = False
    wish_called = False
    wisher_id = False
    for i, line in enumerate(turns):
        if game_ended:
            break

        if line[:7] == "Tichu: ":
            tichu_callers.add(int(line[8]))
            continue

        if line[:7] == "Wunsch:":
            wish_called = line.split(":", 1)[1]
            wisher_id = int(turns[i-1][1])  # the previous line holds the id of the person who wishes a card
            continue

        if line[0] != "(" or line[-6:] == "passt.":
            continue

        player_id = int(line[1])

        for card in line.split(": ", 1)[1].strip().split(" "):
            hands[player_id].remove(card)

        if player_id not in finished_players and len(hands[player_id]) == 0:
            rows[player_id].append(finished_position)
            finished_position += 1
            finished_players.add(player_id)
        
        if len(finished_players) == 2 and (finished_players == set({0,2}) or finished_players == set({1,3})):     
            for id in set({0,1,2,3}) - finished_players:
                rows[id].append(3.5)

            if wish_called:
                rows[wisher_id].append(wish_called)
                for id_ in set({0,1,2,3}) - set({wisher_id}):
                    rows[id_].append(None)
            else:
                for id in set({0,1,2,3}):
                    rows[id].append(None)
            
            game_ended = True
            
            break
        elif len(finished_players) == 3:
            missing_id = (set({0,1,2,3}) - finished_players).pop()
            rows[missing_id].append(4.0)

            if wish_called:
                rows[wisher_id].append(wish_called)
                for id_ in set({0,1,2,3}) - set({wisher_id}):
                    rows[id_].append(None)
            else:
                for id in set({0,1,2,3}):
                    rows[id].append(None)

            game_ended = True
            break
    
    if all(len(row) == 1 for row in rows):
        for row in rows:
            row.append(None)

    return rows, tichu_callers


def score(score_line: str):
    if "Ergebnis" not in score_line:
        print('Ergebnis niet gevonden in score_line')
    scores = score_line.split(":", 1)[1].strip()
    [scores_even, scores_uneven] = list(map(lambda x: int(x.strip()), scores.split(" - ")))
    return scores_even, scores_uneven


def csv_rows(rnd: str, rnd_id: int) -> list[any]:
    rows: list[list[any]] = [[rnd_id] for _ in range(4)]  # round id
    
    head_other_split = rnd.split("---------------Rundenverlauf------------------\n", 1)
    head = head_other_split[0]
    body = head_other_split[1]

    gr_cards_lines_split, start_cards_lines_split, deal_lines_split, bombs_player_id = split_dealing_turn(head)

    # loop over the 4 players (with ids 0,1,2,3) and retrieve their starting turn information
    hands: list[set[str]] = [set(), set(), set(), set()]
    
    for id_ in range(4):
        name, gr_cards, extra_cards, deal = starting_turn_information(
            gr_cards_lines_split[id_], start_cards_lines_split[id_], deal_lines_split[id_], hands, id_
        )
        row = rows[id_]
        row.append(name)
        row.append(list(gr_cards))
        row.append(list(extra_cards))
        row.append(deal["left"])
        row.append(deal["middle"])
        row.append(deal["right"])
        
    for i, hand in enumerate(hands):
        rows[i].append(list(hand.copy()))

    gr_tichu_callers = get_gr_tichu_callers(start_cards_lines_split)
    for id_ in set({0, 1, 2, 3}):
        if id_ in gr_tichu_callers:
            rows[id_].append(1)
        else:
            rows[id_].append(0)

    new_rows, tichu_callers = other_turns_information(body.strip().split("\n")[:-1],
                                                      hands)  # leave out final line including scores
    for id_, row in enumerate(rows):
        rows[id_] += new_rows[id_]

        if id_ in tichu_callers:
            row.append(1)
        else:
            row.append(0)

    scores_even, scores_uneven = score(body.strip().split("\n")[-1])
    for id, row in enumerate(rows):  # score per player
        if id % 2 == 0:
            row.append(scores_even)
        else:
            row.append(scores_uneven)

    for id, row in enumerate(rows):
        if id in bombs_player_id:
            row.append(1)
        else:
            row.append(0)

    return rows

def process_text_file(text_file: str, game_id: str) -> list:
    rounds = text_file.split("---------------Gr.Tichukarten------------------\n")[1:]

    resulting_rows = []
    for i, rnd in enumerate(rounds):
        rnd_rows = csv_rows(rnd, i)  # i is round id
        for row in rnd_rows:
            resulting_rows.append([game_id] + row)
    return resulting_rows
