import json
import os
import pandas as pd
from glob import glob


class CricSheetJsonParser:
    def __init__(self, input_folder, output_file):
        self.input_folder = input_folder
        self.output_file = output_file

    def get_person_id(self, name, registry):
        """Helper to safely fetch ID from the registry."""
        return registry.get("people", {}).get(name)

    def flatten_match(self, data, filename):
        info = data.get("info", {})
        meta = data.get("meta", {})
        registry = info.get("registry", {})
        teams = info.get("teams", [])

        # 1. Metadata & Event Info
        record = {
            "match_id": filename.replace(".json", ""),
            "data_version": meta.get("data_version"),
            "revision": meta.get("revision"),
            "created_at": meta.get("created"),
            "season": info.get("season"),
            "match_type": info.get("match_type"),
            "match_type_number": info.get("match_type_number"),
            "event_name": info.get("event", {}).get("name"),
            "match_number": info.get("event", {}).get("match_number"),
            "city": info.get("city"),
            "venue": info.get("venue"),
            "start_date": info.get("dates", [None])[0],
            "gender": info.get("gender"),
            "team_type": info.get("team_type"),
            "balls_per_over": info.get("balls_per_over"),
            # Toss & Outcome
            "toss_winner": info.get("toss", {}).get("winner"),
            "toss_decision": info.get("toss", {}).get("decision"),
            "outcome_result": info.get("outcome", {}).get("result"),
            "winner": info.get("outcome", {}).get("winner"),
            "win_by_runs": info.get("outcome", {}).get("by", {}).get("runs"),
            "win_by_wickets": info.get("outcome", {}).get("by", {}).get("wickets"),
            "player_of_match": ", ".join(info.get("player_of_match", [])),
        }

        # 2. Player Data (Up to 11 players per team)
        # We use Team 1 and Team 2 based on the 'teams' list order
        for i in range(2):
            team_idx = i + 1
            team_name = teams[i] if i < len(teams) else None
            record[f"team{team_idx}_name"] = team_name

            players = info.get("players", {}).get(team_name, []) if team_name else []
            for p_idx in range(11):
                p_num = p_idx + 1
                name_col = f"team{team_idx}_p{p_num}_name"
                id_col = f"team{team_idx}_p{p_num}_id"

                if p_idx < len(players):
                    p_name = players[p_idx]
                    record[name_col] = p_name
                    record[id_col] = self.get_person_id(p_name, registry)
                else:
                    record[name_col] = None
                    record[id_col] = None

        # 3. Officials Data
        officials = info.get("officials", {})

        # Field Umpires
        umpires = officials.get("umpires", [])
        for i in range(2):
            u_num = i + 1
            u_name = umpires[i] if i < len(umpires) else None
            record[f"umpire_{u_num}_name"] = u_name
            record[f"umpire_{u_num}_id"] = (
                self.get_person_id(u_name, registry) if u_name else None
            )

        # TV Umpire
        tv_u = officials.get("tv_umpires", [None])[0]
        record["tv_umpire_name"] = tv_u
        record["tv_umpire_id"] = self.get_person_id(tv_u, registry) if tv_u else None

        # Match Referee
        ref = officials.get("match_referees", [None])[0]
        record["match_referee_name"] = ref
        record["match_referee_id"] = self.get_person_id(ref, registry) if ref else None

        return record

    def run(self):
        json_files = glob(os.path.join(self.input_folder, "*.json"))
        print(f"Found {len(json_files)} files. Starting processing...")

        all_matches = []
        for i, file_path in enumerate(json_files):
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    row = self.flatten_match(data, os.path.basename(file_path))
                    all_matches.append(row)
            except Exception as e:
                print(f"Error processing {file_path}: {e}")

            if (i + 1) % 1000 == 0:
                print(f"Processed {i + 1} files...")

        # Export to CSV
        df = pd.DataFrame(all_matches)
        df.to_csv(self.output_file, index=False, encoding="utf-8")
        print(f"Success! Saved {len(df)} records to {self.output_file}")


# --- Execution ---
if __name__ == "__main__":
    # Specify your folder path here
    INPUT_DIR = "path/to/your/jsons"
    OUTPUT_FILE = "cricsheet_comprehensive_data.csv"

    parser = CricSheetJsonParser(INPUT_DIR, OUTPUT_FILE)
    parser.run()
