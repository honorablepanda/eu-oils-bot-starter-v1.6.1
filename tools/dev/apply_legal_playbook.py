import pathlib, json, re

ROOT = pathlib.Path(__file__).resolve().parents[2]
retailers = ROOT / "retailers.csv"
created = []

def upsert_csv_lines():
    wanted = {
        "ah_nl": 'ah_nl,Albert Heijn,https://www.ah.nl,https://www.ah.nl/producten/suiker-snoep-koek-chocola/olie/olijfolie,NL,nl-NL,olive,false,"wayback,archive_today,memento",auto,6,,',
        "jumbo_nl": 'jumbo_nl,Jumbo,https://www.jumbo.com,https://www.jumbo.com/producten/olijfolie,NL,nl-NL,olive,true,"wayback,archive_today,memento",auto,6,"button:has-text(\'meer\')" ,,',
        "carrefour_be": 'carrefour_be,Carrefour,https://www.carrefour.be,https://www.carrefour.be/nl/c/olijfolie,BE,nl-BE,olive,false,"wayback,archive_today,memento",auto,6,"button:has-text(\'meer\')" ,,',
        "colruyt_be": 'colruyt_be,Colruyt,https://www.colruyt.be,https://www.colruyt.be/nl/shop/c/olijfolie,BE,nl-BE,olive,false,"wayback,archive_today,memento",auto,6,,Halle,.store-picker,.confirm-store',
    }
    text = retailers.read_text(encoding="utf-8")
    for code, line in wanted.items():
        if re.search(rf"^{code}\b", text, flags=re.M):
            text = re.sub(rf"^{code}.*$", line, text, flags=re.M)
        else:
            text += ("\n" + line)
            created.append(code)
    retailers.write_text(text, encoding="utf-8")

def main():
    assert retailers.exists(), f"Missing {retailers}"
    upsert_csv_lines()
    print("[OK] retailers.csv updated. New entries:", created)

if __name__ == "__main__":
    main()
