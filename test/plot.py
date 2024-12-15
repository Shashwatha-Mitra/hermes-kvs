import json
import matplotlib.pyplot as plt
import numpy as np

def plot_time_vs_throughput(json_file_all_kill, json_fill_4_kill=None):
    try:
        # Read the JSON file
        with open(json_file_all_kill, 'r') as file:
            data = json.load(file)

        # Extract the data for the plot
        x = data["time_v_throughput"]["time"]
        y = data["time_v_throughput"]["throughput"]
        # Extract the data for the plot
        x = np.array(data["time_v_throughput"]["time"])
        y = np.array(data["time_v_throughput"]["throughput"])

        y = (y * 8) / 1000 # number of clients

        # Create the plot
        plt.figure(figsize=(14, 6))
        plt.plot(x, y, linestyle='-', color='b')
        #plt.plot(x, y, marker='o', linestyle='-', color='b', label='Throughput over Time')
        plt.title("Throughput Vs Time")
        plt.xlabel("Time (seconds)")
        plt.ylabel("Throughput (Kops/s)")
        plt.xticks(np.arange(min(x), max(x) + 1, 10))  # Set x-axis ticks in multiples of 10
        #plt.legend()
        plt.grid(True)

        # Save the figure as a PNG file
        plt.savefig("throughput_vs_time.png")
        #plt.show()

    except FileNotFoundError:
        print(f"Error: The file '{json_file}' was not found.")
    except KeyError as e:
        print(f"Error: Missing key in JSON data: {e}")
    except json.JSONDecodeError:
        print("Error: Failed to decode JSON. Ensure the file is correctly formatted.")

# Example usage
if __name__ == "__main__":
    # Replace 'input.json' with the path to your JSON file
    plot_time_vs_throughput('../out/hermes/stats_0.json')