def retrieve_urls(urls, tournament_cards, old_url, new_url):    
    for card in tournament_cards:
        href = card.get("href")
        matches_url = "https://www.vlr.gg" + href.replace(f"{old_url}", f"{new_url}")
        tournament = card.find("div", class_="event-item-title").text.strip().split(": ")
        if len(tournament) == 2:
            tournament_name = tournament[1]
        else:
            tournament_name = tournament[0]
        if tournament_name == "LOCK//IN São Paulo":
            tournament_name = "Lock-In Sao Paulo"

        urls[tournament_name] = matches_url