# Start Streaming Data
Start-Process powershell -ArgumentList "-NoExit", "-Command", "python log_generator_hdfs.py --rate 1200 --batch-interval 1"

# Wait for log generator to initialize
Start-Sleep -Seconds 10

# Start Stream Processing
Start-Process powershell -ArgumentList "-NoExit", "-Command", "while (`$true) { spark-submit .\stream_processor_hdfs.py; Start-Sleep -Seconds 30 }"

# Wait for stream processor startup
Start-Sleep -Seconds 10

# Start Batch Analysis
Start-Process powershell -ArgumentList "-NoExit", "-Command", "while (`$true) { spark-submit .\batch_analysis_hdfs.py; Start-Sleep -Seconds 60 }"

# Wait before dashboard startup
Start-Sleep -Seconds 10

# Start Dashboard
Start-Process powershell -ArgumentList "-NoExit", "-Command", "python -m streamlit run .\dashboard.py"
