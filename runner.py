import subprocess
import time
import datetime

SCRIPT_TO_RUN = "rapid.py"
RUN_DURATION_MINUTES = 60
SLEEP_SECONDS = 300

def run_script(script_name):
    """Menjalankan file Python lain dan tampilkan output secara real-time (UTF-8 safe)."""
    print(f"\n🚀 Menjalankan {script_name} ...")
    process = subprocess.Popen(
        ["python", script_name],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding='utf-8',
        errors='replace'  # hindari crash jika ada karakter tidak dikenal
    )

    # Cetak output real-time
    for line in process.stdout:
        print(line, end="")

    process.wait()

def main():
    start_time = time.time()
    end_time = start_time + RUN_DURATION_MINUTES * 60
    loop = 1

    while time.time() < end_time:
        print("=" * 60)
        print(f"🔁 LOOP KE-{loop} | {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)

        run_script(SCRIPT_TO_RUN)

        print(f"🕒 Menunggu {SLEEP_SECONDS} detik sebelum menjalankan lagi...\n")
        time.sleep(SLEEP_SECONDS)
        loop += 1

    print("✅ Waktu eksekusi habis. Runner selesai.")

if __name__ == "__main__":
    main()
