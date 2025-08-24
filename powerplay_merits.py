import json
import os
import time
import sqlite3
import threading
from datetime import datetime, UTC
from flask import Flask, jsonify, render_template_string

# Load configuration
with open('config.json') as config_file:
    config = json.load(config_file)
LOG_DIRECTORY = config['log_directory']
DATABASE_NAME = config['database_name']
SLEEP_TIME = config["sleep_time"]

# Initialize Flask app
app = Flask(__name__)

# Database setup
def setup_database():
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS events (
            timestamp TEXT PRIMARY KEY,
            power TEXT,
            merits_gained INTEGER,
            total_merits INTEGER
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS processed_logs (
            log_file_name TEXT PRIMARY KEY
        )
    ''')
    conn.commit()
    conn.close()

# Get sorted list of log files
def get_log_files():
    log_files = [f for f in os.listdir(LOG_DIRECTORY) if f.startswith('Journal.') and f.endswith('.log')]
    log_files.sort()  # Sorts by filename, which is chronological due to timestamp format
    return log_files

# Process a single log file
def process_log_file(log_file, conn):
    cursor = conn.cursor()
    file_path = os.path.join(LOG_DIRECTORY, log_file)
    with open(file_path, 'r') as file:
        for line in file:
            try:
                event = json.loads(line.strip())
                if event.get('event') == 'PowerplayMerits':
                    cursor.execute('''
                        INSERT OR IGNORE INTO events (timestamp, power, merits_gained, total_merits)
                        VALUES (?, ?, ?, ?)
                    ''', (event['timestamp'], event['Power'], event['MeritsGained'], event['TotalMerits']))
            except json.JSONDecodeError:
                continue  # Skip invalid lines
    cursor.execute('INSERT OR IGNORE INTO processed_logs (log_file_name) VALUES (?)', (log_file,))
    conn.commit()

# Monitor log files for new events
def monitor_logs():
    # Initialize database connection for this thread
    conn = sqlite3.connect(DATABASE_NAME)
    
    # Load already processed log files
    cursor = conn.cursor()
    cursor.execute('SELECT log_file_name FROM processed_logs')
    processed_logs = set(row[0] for row in cursor.fetchall())
    
    while True:
        log_files = get_log_files()
        if not log_files:
            time.sleep(SLEEP_TIME) #(5)
            continue

        # Process any unprocessed historical log files
        for log_file in log_files[:-1]:  # Exclude the latest log file
            if log_file not in processed_logs:
                process_log_file(log_file, conn)
                processed_logs.add(log_file)

        # Monitor the latest log file
        latest_log = log_files[-1]
        if latest_log not in processed_logs:
            file_path = os.path.join(LOG_DIRECTORY, latest_log)
            with open(file_path, 'r') as file:
                file.seek(0, 2)  # Move to the end of the file
                while True:
                    line = file.readline()
                    if not line:
                        time.sleep(1)  # Wait for new content
                        # Check for a new log file every 5 seconds
                        new_log_files = get_log_files()
                        if new_log_files and new_log_files[-1] != latest_log:
                            # Process remaining lines and switch to new file
                            for remaining_line in file:
                                try:
                                    event = json.loads(remaining_line.strip())
                                    if event.get('event') == 'PowerplayMerits':
                                        cursor.execute('''
                                            INSERT OR IGNORE INTO events (timestamp, power, merits_gained, total_merits)
                                            VALUES (?, ?, ?, ?)
                                        ''', (event['timestamp'], event['Power'], event['MeritsGained'], event['TotalMerits']))
                                except json.JSONDecodeError:
                                    continue
                            conn.commit()
                            cursor.execute('INSERT OR IGNORE INTO processed_logs (log_file_name) VALUES (?)', (latest_log,))
                            conn.commit()
                            break
                        continue
                    try:
                        event = json.loads(line.strip())
                        if event.get('event') == 'PowerplayMerits':
                            cursor.execute('''
                                INSERT OR IGNORE INTO events (timestamp, power, merits_gained, total_merits)
                                VALUES (?, ?, ?, ?)
                            ''', (event['timestamp'], event['Power'], event['MeritsGained'], event['TotalMerits']))
                            conn.commit()
                    except json.JSONDecodeError:
                        continue
    conn.close()

# Web routes
@app.route('/')
def index():
    html = '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Powerplay Merits Dashboard</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
            body { background-color: #333; color: #fff; font-family: Arial, sans-serif; }
            h1 { text-align: center; }
            canvas { background-color: #222; margin: 20px auto; display: block; }
        </style>
    </head>
    <body>
        <h1>Daily Total Merits by Power</h1>
        <canvas id="dailyChart" width="800" height="400"></canvas>
        <h1>Hourly Merits Gained (Current Month)</h1>
        <canvas id="hourlyChart" width="800" height="400"></canvas>
        <script>
            const dailyCtx = document.getElementById('dailyChart').getContext('2d');
            const hourlyCtx = document.getElementById('hourlyChart').getContext('2d');
            let dailyChart, hourlyChart;

            function updateCharts() {
                // Fetch daily total merits
                fetch('/api/daily_total_merits')
                    .then(response => response.json())
                    .then(data => {
                        const days = [...new Set(data.map(row => row[0].split('T')[0]))];
                        const powers = [...new Set(data.map(row => row[1]))];
                        const datasets = powers.map(power => ({
                            label: power,
                            data: days.map(day => {
                                const entry = data.find(row => row[0].split('T')[0] === day && row[1] === power);
                                return entry ? entry[2] : null;
                            }),
                            fill: false,
                            borderColor: '#' + Math.floor(Math.random()*16777215).toString(16),
                            tension: 0.1
                        }));

                        if (dailyChart) dailyChart.destroy();
                        dailyChart = new Chart(dailyCtx, {
                            type: 'line',
                            data: { labels: days, datasets: datasets },
                            options: { scales: { y: { beginAtZero: true, title: { display: true, text: 'Total Merits', color: '#fff' } },
                                               x: { title: { display: true, text: 'Date', color: '#fff' } } },
                                      plugins: { legend: { labels: { color: '#fff' } } } }
                        });
                    });

                // Fetch hourly merits gained
                fetch('/api/hourly_merits_gained')
                    .then(response => response.json())
                    .then(data => {
                        const hours = data.map(row => row[0]);
                        const totals = data.map(row => row[1]);

                        if (hourlyChart) hourlyChart.destroy();
                        hourlyChart = new Chart(hourlyCtx, {
                            type: 'bar',
                            data: { labels: hours, datasets: [{ label: 'Merits Gained', data: totals, backgroundColor: '#00ff00' }] },
                            options: { scales: { y: { beginAtZero: true, title: { display: true, text: 'Merits Gained', color: '#fff' } },
                                               x: { title: { display: true, text: 'Hour (YYYY-MM-DD HH)', color: '#fff' } } },
                                      plugins: { legend: { labels: { color: '#fff' } } } }
                        });
                    });
            }

            setInterval(updateCharts, 20000); // Update every 20 seconds
            updateCharts(); // Initial load
        </script>
    </body>
    </html>
    '''
    return render_template_string(html)

@app.route('/api/daily_total_merits')
def daily_total_merits():
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT e.timestamp, e.power, e.total_merits
        FROM events e
        JOIN (
            SELECT MAX(timestamp) as max_ts, power, date(timestamp) as day
            FROM events
            GROUP BY day, power
        ) sub ON e.timestamp = sub.max_ts AND e.power = sub.power
        ORDER BY sub.day, e.power
    ''')
    data = cursor.fetchall()
    conn.close()
    return jsonify(data)

@app.route('/api/hourly_merits_gained')
def hourly_merits_gained():
    current_month = datetime.now(UTC).strftime('%Y-%m')
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT strftime('%Y-%m-%d %H', timestamp) as hour, SUM(merits_gained) as total_merits_gained
        FROM events
        WHERE timestamp LIKE ? || '%'
        GROUP BY hour
        ORDER BY hour
    ''', (current_month,))
    data = cursor.fetchall()
    conn.close()
    return jsonify(data)

# Main execution
if __name__ == '__main__':
    setup_database()
    # Start log monitoring in a separate thread
    threading.Thread(target=monitor_logs, daemon=True).start()
    # Run Flask app
    app.run(host='0.0.0.0', port=5000, debug=False)
