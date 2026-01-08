import json
import os
import sys
import urllib.request
import urllib.error


def call_goalmodel(api_url: str, home_team: str, away_team: str, competition: str = "Serie A", season: int = 19):
    payload = {
        "home_team": home_team,
        "away_team": away_team,
        "competition": competition,
        "season": season,
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        api_url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        body = resp.read().decode("utf-8")
        return json.loads(body)


def main():
    api_url = os.getenv("GOALMODEL_API_URL", "http://localhost:9001/predict")

    print(f"URL goalmodel: {api_url}")

    tests = [
        ("Inter", "Napoli"),
        ("Milan", "Genoa"),
        ("Juventus", "Cremonese"),
    ]

    for home, away in tests:
        print(f"\n=== Test {home} vs {away} ===")
        try:
            result = call_goalmodel(api_url, home, away)
        except urllib.error.URLError as e:
            print(f"Errore di rete/connessione: {e}")
            continue
        except Exception as e:
            print(f"Errore imprevisto: {e}")
            continue

        required_keys = [
            "home_win_prob",
            "draw_prob",
            "away_win_prob",
            "expected_goals_home",
            "expected_goals_away",
        ]

        missing = [k for k in required_keys if k not in result]
        if missing:
            print(f"Risposta incompleta, mancano i campi: {missing}")
            print("Risposta grezza:", result)
            continue

        print("Probabilità 1-X-2:")
        print(f"  1 (casa) : {result['home_win_prob']}%")
        print(f"  X (pareggio): {result['draw_prob']}%")
        print(f"  2 (trasferta): {result['away_win_prob']}%")

        print("Goal attesi (xG):")
        print(f"  Casa: {result['expected_goals_home']}")
        print(f"  Trasferta: {result['expected_goals_away']}")
        total_xg = result.get("expected_goals_total")
        if total_xg is None:
            total_xg = result["expected_goals_home"] + result["expected_goals_away"]
        print(f"  Totale: {total_xg}")

        btts = result.get("both_teams_to_score_prob")
        over25 = result.get("over_25_prob")
        scoreline = result.get("most_likely_scoreline")
        if btts is not None:
            print(f"Probabilità goal entrambe: {btts}%")
        if over25 is not None:
            print(f"Probabilità Over 2.5: {over25}%")
        if scoreline:
            print(f"Risultato più probabile: {scoreline}")


if __name__ == "__main__":
    sys.exit(main())

