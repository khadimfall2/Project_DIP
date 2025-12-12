import subprocess
import sys

def run_script(script_name):
    """Run a Python script and stream its output."""
    print(f"\n=== Running {script_name} ===")
    try:
        result = subprocess.run([sys.executable, script_name], check=True)
        print(f"=== Finished {script_name} ===\n")
    except subprocess.CalledProcessError as e:
        print(f"Error running {script_name}: {e}")

if __name__ == "__main__":
    print("Starting full benchmark and plotting pipeline...\n")

    # Step 1: Run benchmarking script
    run_script("benchmarkAllphases.py")

    # Step 2: Run plotting script
    run_script("Plot_of_Benchmark.py")

    print("All results generated. Check benchmark_results.csv and the chart PNG file.")