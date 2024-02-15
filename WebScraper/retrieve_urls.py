def retrieve_urls(urls, tournament_ids, tournament_cards, old_url, new_url):    
    for card in tournament_cards:
        href = card.get("href")
        tournament_id = href.split("/")[2]
        matches_url = "https://www.vlr.gg" + href.replace(f"{old_url}", f"{new_url}")
        tournament = card.find("div", class_="event-item-title").text.strip().split(": ")
        if len(tournament) == 2 and tournament[1] == "LOCK//IN São Paulo":
            tournament[1] = "Lock-In Sao Paulo"
        tournament_name = ": ".join(tournament)
        urls[tournament_name] = matches_url
        tournament_ids[tournament_name] = tournament_id