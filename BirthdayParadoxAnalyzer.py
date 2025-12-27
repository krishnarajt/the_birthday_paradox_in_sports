import pandas as pd


class BirthdayParadoxAnalyzer:
    def __init__(self, file_path):
        self.df = pd.read_csv(file_path)
        self.squad_size = 23
        self._preprocess()

    def _preprocess(self):
        # Convert DOB to datetime and extract Month-Day
        self.df["DOB_dt"] = pd.to_datetime(
            self.df["DOB"], dayfirst=True, errors="coerce"
        )
        self.df["birthday_md"] = self.df["DOB_dt"].dt.strftime("%m-%d")
        # Ensure # column is numeric for filtering
        self.df["#"] = pd.to_numeric(self.df["#"], errors="coerce")

    def run_analysis(self):
        # Filter for players numbered 1-23 in each squad (src_page)
        players = self.df[(self.df["#"] >= 1) & (self.df["#"] <= 23)].copy()
        squads = players.groupby("src_page")

        total_squads = 0
        squads_with_matches = 0

        print("--- Shared Birthday Details ---")
        for squad_id, squad_data in squads:
            squad_players = squad_data.sort_values("#").head(self.squad_size)
            if len(squad_players) < self.squad_size:
                continue

            total_squads += 1
            bday_counts = squad_players["birthday_md"].value_counts()
            shared_bdays = bday_counts[bday_counts > 1].index.tolist()

            if shared_bdays:
                squads_with_matches += 1
                print(f"\nSquad (Page {squad_id}) has matches:")
                for bday in shared_bdays:
                    matching_players = squad_players[
                        squad_players["birthday_md"] == bday
                    ]
                    names = matching_players["PLAYER NAME"].tolist()
                    print(f"  - {bday}: {', '.join(names)}")

        # Calculations
        observed_prob = (
            (squads_with_matches / total_squads) * 100 if total_squads > 0 else 0
        )
        theory_prob = self.calculate_theoretical_probability(23) * 100

        self.print_summary(
            total_squads, squads_with_matches, observed_prob, theory_prob
        )

    def calculate_theoretical_probability(self, n=23):
        prob_no_match = 1.0
        for i in range(n):
            prob_no_match *= (365 - i) / 365
        return 1 - prob_no_match

    def print_summary(self, total, matches, observed, theory):
        print("\n" + "=" * 40)
        print("DETAILED ANALYSIS")
        print("=" * 40)
        print(f"Total squads analyzed: {total}")
        print(f"Squads with shared birthdays: {matches}")
        print(f"Observed Frequency: {observed:.2f}%")
        print(f"Theoretical Probability: {theory:.2f}%")
        print("-" * 40)
        diff = abs(observed - theory)
        conclusion = (
            "Data aligns closely."
            if diff < 5
            else "Notable deviation due to small sample size."
        )
        print(f"Analysis: {conclusion}")


# Execution
