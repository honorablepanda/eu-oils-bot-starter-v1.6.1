import time, requests, argparse
SPN = "https://web.archive.org/save/"

def main(urls):
    for u in urls:
        try:
            r = requests.get(SPN + u, timeout=20)
            print(f"[SPN] {u} → {r.status_code}")
            time.sleep(2.0)
        except Exception as e:
            print(f"[SPN] {u} ! {e}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True)
    args = ap.parse_args()
    with open(args.file, "r", encoding="utf-8") as f:
        main([ln.strip() for ln in f if ln.strip()])
